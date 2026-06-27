// Package valkey provides a Valkey cache store implementation.
//
// PLUGGABLE ARCHITECTURE: This demonstrates how easily cache backends can be swapped.
// Switching from Redis to Valkey requires only changing the config - no application code changes.
//
//	cache.New(cache.CacheRedis, &cache.RedisConfig{...})  // Redis
//	cache.New(cache.CacheValkey, &cache.ValkeyConfig{...}) // Valkey - same interface!
//
// Valkey is a high-performance Redis fork with 100% protocol compatibility.
package valkey

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	slogger "log/slog"

	"github.com/cenkalti/backoff/v4"
	"github.com/valkey-io/valkey-go"
	store "github.com/vinaycharlie01/nyro/carts"
)

const (
	// ValkeyType represents the storage type
	ValkeyType = "valkey"

	// Lock configuration defaults
	defaultLockTTL            = 10 * time.Second
	defaultLockMaxWait        = 3 * time.Second
	defaultLockInitialBackoff = 50 * time.Millisecond
	defaultLockMaxBackoff     = 500 * time.Millisecond
	lockKeySuffix             = ":lock"

	// Lua script for safe lock release (only delete if value matches)
	releaseLockScript = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
)

// ValkeyStoreConfig holds configuration for Valkey store
type ValkeyStoreConfig struct {
	LockTTL             time.Duration
	LockMaxWait         time.Duration
	LockInitialBackoff  time.Duration
	LockMaxBackoff      time.Duration
	LockMultiplier      float64
	LockRenewalInterval time.Duration
}

// DefaultValkeyStoreConfig returns default configuration
func DefaultValkeyStoreConfig() *ValkeyStoreConfig {
	return &ValkeyStoreConfig{
		LockTTL:            defaultLockTTL,
		LockMaxWait:        defaultLockMaxWait,
		LockInitialBackoff: defaultLockInitialBackoff,
		LockMaxBackoff:     defaultLockMaxBackoff,
		LockMultiplier:     2.0,
	}
}

// ValkeyStore implements the Store interface for Valkey.
type ValkeyStore struct {
	client valkey.Client
	config *ValkeyStoreConfig
}

// ValkeyStoreOption is a functional option for configuring ValkeyStore
type ValkeyStoreOption func(*ValkeyStoreConfig)

// WithLockTTL sets the TTL for distributed locks
func WithLockTTL(ttl time.Duration) ValkeyStoreOption {
	return func(c *ValkeyStoreConfig) {
		c.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum time to wait for lock holder to finish
func WithLockMaxWait(d time.Duration) ValkeyStoreOption {
	return func(c *ValkeyStoreConfig) {
		c.LockMaxWait = d
	}
}

// WithLockBackoff sets the initial and maximum backoff intervals
func WithLockBackoff(initial, max time.Duration) ValkeyStoreOption {
	return func(c *ValkeyStoreConfig) {
		c.LockInitialBackoff = initial
		c.LockMaxBackoff = max
	}
}

// WithLockMultiplier sets the backoff multiplier
func WithLockMultiplier(multiplier float64) ValkeyStoreOption {
	return func(c *ValkeyStoreConfig) {
		c.LockMultiplier = multiplier
	}
}

// NewValkey creates a new Valkey store instance
func NewValkey(client valkey.Client, opts ...ValkeyStoreOption) *ValkeyStore {
	config := DefaultValkeyStoreConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &ValkeyStore{
		client: client,
		config: config,
	}
}

// Get retrieves a value from Valkey
func (s *ValkeyStore) Get(ctx context.Context, key string) (any, error) {
	cmd := s.client.B().Get().Key(key).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, store.NotFoundWithCause(err)
		}
		return nil, fmt.Errorf("valkey get failed: %w", err)
	}

	str, err := result.ToString()
	if err != nil {
		return nil, fmt.Errorf("failed to convert result to string: %w", err)
	}

	var value any
	if err := json.Unmarshal([]byte(str), &value); err != nil {
		return str, nil
	}
	return value, nil
}

// Set stores a value in Valkey with expiration
func (s *ValkeyStore) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	cmd := s.client.B().Set().Key(key).Value(string(data)).ExSeconds(int64(expiration.Seconds())).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey set failed: %w", err)
	}

	return nil
}

// Delete removes a key from Valkey
func (s *ValkeyStore) Delete(ctx context.Context, key string) error {
	cmd := s.client.B().Del().Key(key).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey delete failed: %w", err)
	}
	return nil
}

// Exists checks if a key exists in Valkey
func (s *ValkeyStore) Exists(ctx context.Context, key string) (bool, error) {
	cmd := s.client.B().Exists().Key(key).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		return false, fmt.Errorf("valkey exists failed: %w", err)
	}

	count, err := result.AsInt64()
	if err != nil {
		return false, fmt.Errorf("failed to parse exists result: %w", err)
	}

	return count > 0, nil
}

// GetMulti retrieves multiple keys from Valkey
func (s *ValkeyStore) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	if len(keys) == 0 {
		return make(map[string]any), nil
	}

	cmd := s.client.B().Mget().Key(keys...).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		return nil, fmt.Errorf("valkey mget failed: %w", err)
	}

	values, err := result.AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("failed to parse mget result: %w", err)
	}

	resultMap := make(map[string]any, len(keys))
	for i, key := range keys {
		if i >= len(values) {
			break
		}

		strVal := values[i]
		if strVal == "" {
			continue
		}

		var value any
		if err := json.Unmarshal([]byte(strVal), &value); err != nil {
			resultMap[key] = strVal
		} else {
			resultMap[key] = value
		}
	}

	return resultMap, nil
}

// SetMulti sets multiple keys in Valkey
func (s *ValkeyStore) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	for key, value := range items {
		if err := s.Set(ctx, key, value, expiration); err != nil {
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return nil
}

// DeleteMulti removes multiple keys from Valkey
func (s *ValkeyStore) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	cmd := s.client.B().Del().Key(keys...).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey del failed: %w", err)
	}

	return nil
}

// Clear flushes all keys from Valkey
func (s *ValkeyStore) Clear(ctx context.Context) error {
	cmd := s.client.B().Flushall().Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey flushall failed: %w", err)
	}
	return nil
}

// GetType returns the store type identifier
func (s *ValkeyStore) GetType() string {
	return ValkeyType
}

// HealthCheck verifies Valkey connectivity
func (s *ValkeyStore) HealthCheck(ctx context.Context) error {
	cmd := s.client.B().Ping().Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey health check failed: %w", err)
	}
	return nil
}

// Close closes the Valkey client connection
func (s *ValkeyStore) Close() error {
	s.client.Close()
	return nil
}

// AcquireLock attempts to acquire a distributed lock
func (s *ValkeyStore) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockKey := getLockKey(key)
	lockValue = generateLockValue()

	cmd := s.client.B().Set().Key(lockKey).Value(lockValue).Nx().ExSeconds(int64(ttl.Seconds())).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		return "", false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	str, err := result.ToString()
	acquired = (err == nil && str == "OK")

	return lockValue, acquired, nil
}

// ReleaseLock releases a distributed lock SAFELY
func (s *ValkeyStore) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	lockKey := getLockKey(key)

	cmd := s.client.B().Eval().Script(releaseLockScript).Numkeys(1).Key(lockKey).Arg(lockValue).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock
func (s *ValkeyStore) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	lockKey := getLockKey(key)

	extendLockScript := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	cmd := s.client.B().Eval().Script(extendLockScript).Numkeys(1).Key(lockKey).Arg(lockValue).Arg(fmt.Sprintf("%d", int(ttl.Seconds()))).Build()
	result := s.client.Do(ctx, cmd)

	if err := result.Error(); err != nil {
		return false, fmt.Errorf("failed to extend lock: %w", err)
	}

	count, err := result.AsInt64()
	if err != nil {
		return false, fmt.Errorf("failed to parse extend result: %w", err)
	}

	return count == 1, nil
}

// GetOrSetWithLock retrieves a value or sets it with distributed locking
func (s *ValkeyStore) GetOrSetWithLock(ctx context.Context, key string, loader func(context.Context) (any, error), expiration time.Duration, lockTTL time.Duration) (any, error) {
	value, err := s.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFoundErr *store.NotFound
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("cache get error: %w", err)
	}

	if lockTTL == 0 {
		lockTTL = s.config.LockTTL
	}

	lockValue, acquired, err := s.AcquireLock(ctx, key, lockTTL)
	if err != nil {
		return nil, fmt.Errorf("lock acquisition failed: %w", err)
	}

	if !acquired {
		return s.waitForCache(ctx, key)
	}

	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.ReleaseLock(releaseCtx, key, lockValue); err != nil {
			slogger.Error("failed to release lock", "key", key, "error", err)
		}
	}()

	value, err = s.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var doubleCheckNotFound *store.NotFound
	if !errors.As(err, &doubleCheckNotFound) {
		return nil, fmt.Errorf("cache get error during double-check: %w", err)
	}

	renewalInterval := s.config.LockRenewalInterval
	if renewalInterval <= 0 {
		renewalInterval = lockTTL / 3
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	go s.startLockHeartbeat(heartbeatCtx, key, lockValue, lockTTL, renewalInterval)

	result, err := loader(ctx)
	cancelHeartbeat()

	if err != nil {
		return nil, fmt.Errorf("loader failed: %w", err)
	}

	if result == nil {
		return nil, errors.New("loader returned nil result")
	}

	if err := s.Set(ctx, key, result, expiration); err != nil {
		slogger.Error("failed to set cache after loading", "key", key, "error", err)
	}

	return result, nil
}

func (s *ValkeyStore) waitForCache(ctx context.Context, key string) (any, error) {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = s.config.LockInitialBackoff
	expBackoff.MaxInterval = s.config.LockMaxBackoff
	expBackoff.MaxElapsedTime = s.config.LockMaxWait
	expBackoff.Multiplier = s.config.LockMultiplier
	expBackoff.RandomizationFactor = 0.5

	ctxBackoff := backoff.WithContext(expBackoff, ctx)

	var value any
	operation := func() error {
		v, err := s.Get(ctx, key)
		if err == nil {
			value = v
			return nil
		}

		var notFound *store.NotFound
		if errors.As(err, &notFound) {
			return err
		}

		return backoff.Permanent(err)
	}

	err := backoff.Retry(operation, ctxBackoff)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("timeout waiting for cache population after %v", s.config.LockMaxWait)
		case errors.Is(err, context.Canceled):
			return nil, err
		default:
			return nil, fmt.Errorf("waiting for cache population failed: %w", err)
		}
	}

	return value, nil
}

func getLockKey(key string) string {
	return key + lockKeySuffix
}

func generateLockValue() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}

func (s *ValkeyStore) startLockHeartbeat(ctx context.Context, key string, lockValue string, lockTTL time.Duration, renewalInterval time.Duration) {
	ticker := time.NewTicker(renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			extended, err := s.ExtendLock(ctx, key, lockValue, lockTTL)
			if err != nil || !extended {
				slogger.Warn("lock heartbeat failed", "key", key, "error", err, "extended", extended)
				return
			}
		}
	}
}
