package redis

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
	redis "github.com/redis/go-redis/v9"
	store "github.com/vinaycharlie01/nyro/stores"
	"golang.org/x/sync/singleflight"
)

const (
	// RedisType represents the storage type
	RedisType = "redis"

	// Lock configuration defaults
	defaultLockTTL            = 10 * time.Second
	defaultLockMaxWait        = 3 * time.Second // Max time to wait for lock holder to finish
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

// RedisStoreConfig holds configuration for Redis store
type RedisStoreConfig struct {
	// Lock configuration for distributed locking (cache stampede prevention)
	LockTTL             time.Duration // TTL for distributed locks (default: 10s)
	LockMaxWait         time.Duration // Maximum time to wait for lock holder to finish (default: 3s)
	LockInitialBackoff  time.Duration // Initial backoff interval (default: 50ms)
	LockMaxBackoff      time.Duration // Maximum backoff interval (default: 500ms)
	LockMultiplier      float64       // Backoff multiplier (default: 2.0)
	LockRenewalInterval time.Duration // Interval for lock renewal heartbeat (default: LockTTL/3)
}

// DefaultRedisStoreConfig returns default configuration
func DefaultRedisStoreConfig() *RedisStoreConfig {
	return &RedisStoreConfig{
		LockTTL:            defaultLockTTL,
		LockMaxWait:        defaultLockMaxWait,
		LockInitialBackoff: defaultLockInitialBackoff,
		LockMaxBackoff:     defaultLockMaxBackoff,
		LockMultiplier:     2.0,
	}
}

// RedisClientInterface abstracts redis client operations for testing
type RedisClientInterface interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	MSet(ctx context.Context, values ...any) *redis.StatusCmd
	FlushAll(ctx context.Context) *redis.StatusCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
	Close() error
}

// RedisStore implements Store and DistributedLocker interfaces for Redis
// Supports AWS Redis Cluster and handles high concurrency with distributed locking
type RedisStore struct {
	client RedisClientInterface
	config *RedisStoreConfig
	group  singleflight.Group
}

// RedisStoreOption is a functional option for configuring RedisStore
type RedisStoreOption func(*RedisStoreConfig)

// WithLockTTL sets the TTL for distributed locks
func WithLockTTL(ttl time.Duration) RedisStoreOption {
	return func(c *RedisStoreConfig) {
		c.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum time to wait for lock holder to finish
func WithLockMaxWait(d time.Duration) RedisStoreOption {
	return func(c *RedisStoreConfig) {
		c.LockMaxWait = d
	}
}

// WithLockBackoff sets the initial and maximum backoff intervals
func WithLockBackoff(initial, max time.Duration) RedisStoreOption {
	return func(c *RedisStoreConfig) {
		c.LockInitialBackoff = initial
		c.LockMaxBackoff = max
	}
}

// WithLockMultiplier sets the backoff multiplier
func WithLockMultiplier(multiplier float64) RedisStoreOption {
	return func(c *RedisStoreConfig) {
		c.LockMultiplier = multiplier
	}
}

// NewRedis creates a new Redis store instance
// Compatible with AWS Redis Cluster
//
// Example:
//
//	// With default config
//	store := redis.NewRedis(client)
//
//	// With custom lock configuration
//	store := redis.NewRedis(client,
//		redis.WithLockTTL(15*time.Second),
//		redis.WithLockMaxElapsed(5*time.Second),
//		redis.WithLockBackoff(100*time.Millisecond, 1*time.Second),
//	)
func NewRedis(client RedisClientInterface, opts ...RedisStoreOption) *RedisStore {
	config := DefaultRedisStoreConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &RedisStore{
		client: client,
		config: config,
	}
}

// Get retrieves a value from Redis
func (s *RedisStore) Get(ctx context.Context, key string) (any, error) {
	result, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.NotFoundWithCause(err)
		}
		return nil, fmt.Errorf("redis get failed: %w", err)
	}

	// Deserialize JSON
	var value any
	if err := json.Unmarshal([]byte(result), &value); err != nil {
		// If not JSON, return raw string
		return result, nil
	}
	return value, nil
}

// Set stores a value in Redis with expiration
func (s *RedisStore) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	// Serialize to JSON for complex types
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	if err := s.client.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}

	return nil
}

// Delete removes a key from Redis
func (s *RedisStore) Delete(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis delete failed: %w", err)
	}
	return nil
}

// Exists checks if a key exists in Redis
func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	result, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists failed: %w", err)
	}
	return result > 0, nil
}

// GetMulti retrieves multiple keys from Redis
func (s *RedisStore) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	if len(keys) == 0 {
		return make(map[string]any), nil
	}

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget failed: %w", err)
	}

	result := make(map[string]any, len(keys))
	for i, key := range keys {
		if values[i] == nil {
			continue // Skip nil values (key not found)
		}

		strVal, ok := values[i].(string)
		if !ok {
			continue
		}

		// Try to deserialize JSON
		var value any
		if err := json.Unmarshal([]byte(strVal), &value); err != nil {
			// If not JSON, use raw string
			result[key] = strVal
		} else {
			result[key] = value
		}
	}

	return result, nil
}

// SetMulti sets multiple keys in Redis
func (s *RedisStore) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	// Build MSET arguments
	args := make([]any, 0, len(items)*2)
	for key, value := range items {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}
		args = append(args, key, data)
	}

	// Use MSET for atomic multi-set
	if err := s.client.MSet(ctx, args...).Err(); err != nil {
		return fmt.Errorf("redis mset failed: %w", err)
	}

	// Set expiration for each key (Redis doesn't support MSET with expiration)
	if expiration > 0 {
		for key := range items {
			if err := s.client.Expire(ctx, key, expiration).Err(); err != nil {
				// Log but don't fail - data is already set
				return fmt.Errorf("redis expire failed for key %s: %w", key, err)
			}
		}
	}

	return nil
}

// DeleteMulti removes multiple keys from Redis
func (s *RedisStore) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := s.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}

	return nil
}

// Clear flushes all keys from Redis
// WARNING: Use with caution in production!
func (s *RedisStore) Clear(ctx context.Context) error {
	if err := s.client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("redis flushall failed: %w", err)
	}
	return nil
}

// GetType returns the store type identifier
func (s *RedisStore) GetType() string {
	return RedisType
}

// HealthCheck verifies Redis connectivity
func (s *RedisStore) HealthCheck(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}
	return nil
}

// Close closes the Redis client connection
func (s *RedisStore) Close() error {
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("redis close failed: %w", err)
	}
	return nil
}

// =============================================================================
// Distributed Locking Implementation (Cache Stampede Prevention)
// =============================================================================

// AcquireLock attempts to acquire a distributed lock using Redis SET NX EX
// This prevents cache stampede by ensuring only one process loads data
//
// Lock pattern: SET key value NX EX ttl
// - NX: Only set if key doesn't exist
// - EX: Set expiration in seconds
//
// Returns:
// - lockValue: unique value for safe lock release (ownership check)
// - acquired: true if lock was acquired
// - error: any error during lock acquisition
func (s *RedisStore) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockKey := getLockKey(key)
	lockValue = generateLockValue()

	// Use SET NX EX for atomic lock acquisition
	result, err := s.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return lockValue, result, nil
}

// ReleaseLock releases a distributed lock SAFELY
// Only deletes the lock if the value matches (ownership check)
// This prevents accidentally deleting another process's lock
func (s *RedisStore) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	lockKey := getLockKey(key)

	// Use Lua script for atomic check-and-delete
	result, err := s.client.Eval(ctx, releaseLockScript, []string{lockKey}, lockValue).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	// result is 1 if deleted, 0 if value didn't match
	if result == int64(0) {
		// Lock was already released or acquired by another process
		// This is not an error - just means lock expired
		return nil
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock (for heartbeat/renewal)
// This should be called periodically for long-running operations to prevent lock expiry
func (s *RedisStore) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	lockKey := getLockKey(key)

	// Use Lua script to extend only if lock value matches (ownership check)
	extendLockScript := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := s.client.Eval(ctx, extendLockScript, []string{lockKey}, lockValue, int(ttl.Seconds())).Result()
	if err != nil {
		return false, fmt.Errorf("failed to extend lock: %w", err)
	}

	// result is 1 if extended, 0 if value didn't match
	return result == int64(1), nil
}

// GetOrSetWithLock retrieves a value or sets it with distributed locking.
//
// Flow:
//  1. Try cache
//  2. Cache miss -> acquire distributed lock
//  3. Lock owner loads and populates cache
//  4. Others wait for cache population
func (s *RedisStore) GetOrSetWithLock(ctx context.Context, key string, loader func(context.Context) (any, error), expiration time.Duration, lockTTL time.Duration) (any, error) {
	// Fast path: cache hit
	value, err := s.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFoundErr *store.NotFound
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("cache get error: %w", err)
	}

	// Process-local deduplication for concurrent misses on the same key.
	resultCh := s.group.DoChan(key, func() (any, error) {
		return s.getOrSetWithDistributedLock(ctx, key, loader, expiration, lockTTL)
	})

	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RedisStore) getOrSetWithDistributedLock(ctx context.Context, key string, loader func(context.Context) (any, error), expiration time.Duration, lockTTL time.Duration) (any, error) {

	// Cache miss
	if lockTTL == 0 {
		lockTTL = s.config.LockTTL
	}

	lockValue, acquired, err := s.AcquireLock(ctx, key, lockTTL)
	if err != nil {
		return nil, fmt.Errorf("lock acquisition failed: %w", err)
	}

	// Another node/process owns the lock
	if !acquired {
		return s.waitForCache(ctx, key)
	}

	// We own the lock
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.ReleaseLock(releaseCtx, key, lockValue); err != nil {
			slogger.Error(
				"failed to release lock",
				"key", key,
				"error", err,
			)
		}
	}()

	// Double-check cache after acquiring lock.
	// Another process may have populated cache while we were acquiring.
	cachedValue, err := s.Get(ctx, key)
	if err == nil {
		return cachedValue, nil
	}

	var doubleCheckNotFound *store.NotFound
	if !errors.As(err, &doubleCheckNotFound) {
		return nil, fmt.Errorf(
			"cache get error during double-check: %w",
			err,
		)
	}

	// Heartbeat for long-running loaders
	renewalInterval := s.config.LockRenewalInterval
	if renewalInterval <= 0 {
		renewalInterval = lockTTL / 3
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)

	go s.startLockHeartbeat(heartbeatCtx, key, lockValue, lockTTL, renewalInterval)

	// Execute loader
	result, err := loader(ctx)

	// Stop heartbeat immediately when loader finishes
	cancelHeartbeat()

	if err != nil {
		return nil, fmt.Errorf("loader failed: %w", err)
	}

	// Optional safety check
	if result == nil {
		return nil, errors.New("loader returned nil result")
	}

	// Cache population failure should not fail request
	if err := s.Set(ctx, key, result, expiration); err != nil {
		slogger.Error(
			"failed to set cache after loading",
			"key", key,
			"error", err,
		)
	}

	return result, nil
}

func (s *RedisStore) waitForCache(ctx context.Context, key string) (any, error) {
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

// getLockKey generates a lock key from a cache key
func getLockKey(key string) string {
	return key + lockKeySuffix
}

// generateLockValue generates a unique lock value for ownership tracking
func generateLockValue() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp if random fails
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}

// startLockHeartbeat starts a background goroutine to periodically renew the lock
// This prevents lock expiry during long-running loader operations
func (s *RedisStore) startLockHeartbeat(ctx context.Context, key string, lockValue string, lockTTL time.Duration, renewalInterval time.Duration) {
	ticker := time.NewTicker(renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled - stop heartbeat
			return
		case <-ticker.C:
			// Extend lock to prevent expiry during long-running loader
			extended, err := s.ExtendLock(ctx, key, lockValue, lockTTL)
			if err != nil || !extended {
				// Lock lost - loader should be aware but continue
				slogger.Warn("lock heartbeat failed", "key", key, "error", err, "extended", extended)
				return
			}
		}
	}
}
