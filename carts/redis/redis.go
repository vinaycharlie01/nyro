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
	cart "github.com/vinaycharlie01/nyro/carts"
	"golang.org/x/sync/singleflight"
)

const (
	// RedisType is the identifier for the Redis cart backend.
	RedisType = "redis"

	defaultLockTTL            = 10 * time.Second
	defaultLockMaxWait        = 3 * time.Second
	defaultLockInitialBackoff = 50 * time.Millisecond
	defaultLockMaxBackoff     = 500 * time.Millisecond
	lockKeySuffix             = ":lock"
	lockValueBytes            = 16
	defaultLockMultiplier     = 2.0
	lockRenewalDivisor        = 3
	defaultReleaseLockTimeout = 5 * time.Second

	releaseLockScript = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	extendLockScript = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
)

// RedisCartConfig holds configuration for the Redis cart backend.
type RedisCartConfig struct {
	// LockTTL is the TTL for distributed locks (default: 10s).
	LockTTL time.Duration
	// LockMaxWait is the maximum time to wait for a lock holder to finish (default: 3s).
	LockMaxWait time.Duration
	// LockInitialBackoff is the initial backoff interval (default: 50ms).
	LockInitialBackoff time.Duration
	// LockMaxBackoff is the maximum backoff interval (default: 500ms).
	LockMaxBackoff time.Duration
	// LockMultiplier is the exponential backoff multiplier (default: 2.0).
	LockMultiplier float64
	// LockRenewalInterval is the heartbeat renewal interval (default: LockTTL/3).
	LockRenewalInterval time.Duration
}

// DefaultRedisCartConfig returns the default Redis cart configuration.
func DefaultRedisCartConfig() *RedisCartConfig {
	return &RedisCartConfig{
		LockTTL:            defaultLockTTL,
		LockMaxWait:        defaultLockMaxWait,
		LockInitialBackoff: defaultLockInitialBackoff,
		LockMaxBackoff:     defaultLockMaxBackoff,
		LockMultiplier:     defaultLockMultiplier,
	}
}

// RedisClientInterface abstracts Redis client operations for testing.
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

// RedisCart implements cart.Cart and cart.DistributedLocker for Redis.
// Compatible with AWS ElastiCache; prevents cache stampede via distributed locking.
type RedisCart struct {
	client RedisClientInterface
	config *RedisCartConfig
	group  singleflight.Group
}

// RedisCartOption is a functional option for configuring RedisCart.
type RedisCartOption func(*RedisCartConfig)

// WithLockTTL sets the TTL for distributed locks.
func WithLockTTL(ttl time.Duration) RedisCartOption {
	return func(c *RedisCartConfig) {
		c.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum time to wait for a lock holder to finish.
func WithLockMaxWait(d time.Duration) RedisCartOption {
	return func(c *RedisCartConfig) {
		c.LockMaxWait = d
	}
}

// WithLockBackoff sets the initial and maximum backoff intervals.
func WithLockBackoff(initial, max time.Duration) RedisCartOption {
	return func(c *RedisCartConfig) {
		c.LockInitialBackoff = initial
		c.LockMaxBackoff = max
	}
}

// WithLockMultiplier sets the exponential backoff multiplier.
func WithLockMultiplier(multiplier float64) RedisCartOption {
	return func(c *RedisCartConfig) {
		c.LockMultiplier = multiplier
	}
}

// NewRedis creates a new RedisCart instance.
func NewRedis(client RedisClientInterface, opts ...RedisCartOption) *RedisCart {
	cfg := DefaultRedisCartConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	return &RedisCart{
		client: client,
		config: cfg,
	}
}

// Get retrieves a value from Redis.
func (s *RedisCart) Get(ctx context.Context, key string) (any, error) {
	result, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, cart.NotFoundWithCause(err)
		}

		return nil, fmt.Errorf("redis get failed: %w", err)
	}

	var value any
	if err := json.Unmarshal([]byte(result), &value); err != nil {
		return result, nil
	}

	return value, nil
}

// Set stores a value in Redis with the given expiration.
func (s *RedisCart) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	if err := s.client.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}

	return nil
}

// Delete removes a key from Redis.
func (s *RedisCart) Delete(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis delete failed: %w", err)
	}

	return nil
}

// Exists checks if a key exists in Redis.
func (s *RedisCart) Exists(ctx context.Context, key string) (bool, error) {
	result, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists failed: %w", err)
	}

	return result > 0, nil
}

// GetMulti retrieves multiple keys from Redis in a single MGet call.
func (s *RedisCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
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
			continue
		}

		strVal, ok := values[i].(string)
		if !ok {
			continue
		}

		var value any
		if err := json.Unmarshal([]byte(strVal), &value); err != nil {
			result[key] = strVal
		} else {
			result[key] = value
		}
	}

	return result, nil
}

// SetMulti stores multiple key-value pairs in Redis using MSET.
func (s *RedisCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	args := make([]any, 0, len(items)*2)

	for key, value := range items {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		args = append(args, key, data)
	}

	if err := s.client.MSet(ctx, args...).Err(); err != nil {
		return fmt.Errorf("redis mset failed: %w", err)
	}

	if expiration > 0 {
		for key := range items {
			if err := s.client.Expire(ctx, key, expiration).Err(); err != nil {
				return fmt.Errorf("redis expire failed for key %s: %w", key, err)
			}
		}
	}

	return nil
}

// DeleteMulti removes multiple keys from Redis in a single DEL call.
func (s *RedisCart) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if err := s.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}

	return nil
}

// Clear removes all keys from Redis. Use with caution in production.
func (s *RedisCart) Clear(ctx context.Context) error {
	if err := s.client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("redis flushall failed: %w", err)
	}

	return nil
}

// GetType returns the Redis cart type identifier.
func (s *RedisCart) GetType() string {
	return RedisType
}

// HealthCheck verifies Redis connectivity via PING.
func (s *RedisCart) HealthCheck(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}

	return nil
}

// Close closes the Redis client connection.
func (s *RedisCart) Close() error {
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("redis close failed: %w", err)
	}

	return nil
}

// AcquireLock attempts to acquire a distributed lock using Redis SET NX EX.
func (s *RedisCart) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockKey := getLockKey(key)
	lockValue = generateLockValue()

	result, err := s.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return lockValue, result, nil
}

// ReleaseLock releases a distributed lock safely via a Lua ownership check.
func (s *RedisCart) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	lockKey := getLockKey(key)

	if _, err := s.client.Eval(ctx, releaseLockScript, []string{lockKey}, lockValue).Result(); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock via a Lua ownership check.
// Returns true if the lock was extended, false if it is no longer owned.
func (s *RedisCart) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	lockKey := getLockKey(key)

	result, err := s.client.Eval(ctx, extendLockScript, []string{lockKey}, lockValue, int(ttl.Seconds())).Result()
	if err != nil {
		return false, fmt.Errorf("failed to extend lock: %w", err)
	}

	return result == int64(1), nil
}

// GetOrSetWithLock retrieves a cached value or populates it with distributed lock protection.
func (s *RedisCart) GetOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	value, err := s.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFoundErr *cart.NotFound
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("cache get error: %w", err)
	}

	resultCh := s.group.DoChan(key, func() (any, error) {
		return s.getOrSetWithLock(ctx, key, loader, expiration, lockTTL)
	})

	select {
	case r := <-resultCh:
		if r.Err != nil {
			return nil, r.Err
		}

		return r.Val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

//nolint:cyclop
func (s *RedisCart) getOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
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
		releaseCtx, cancel := context.WithTimeout(context.Background(), defaultReleaseLockTimeout)
		defer cancel()

		if releaseErr := s.ReleaseLock(releaseCtx, key, lockValue); releaseErr != nil {
			slogger.Error("failed to release lock", "key", key, "error", releaseErr)
		}
	}()

	if cachedValue, getErr := s.Get(ctx, key); getErr == nil {
		return cachedValue, nil
	}

	renewalInterval := s.config.LockRenewalInterval
	if renewalInterval <= 0 {
		renewalInterval = lockTTL / lockRenewalDivisor
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)

	go s.startLockHeartbeat(heartbeatCtx, key, lockValue, lockTTL, renewalInterval)

	result, loaderErr := loader(ctx)
	cancelHeartbeat()

	if loaderErr != nil {
		return nil, fmt.Errorf("loader failed: %w", loaderErr)
	}

	if result == nil {
		return nil, errors.New("loader returned nil result")
	}

	if setErr := s.Set(ctx, key, result, expiration); setErr != nil {
		slogger.Error("failed to set cache after loading", "key", key, "error", setErr)
	}

	return result, nil
}

//nolint:dupl
func (s *RedisCart) waitForCache(ctx context.Context, key string) (any, error) {
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

		var notFound *cart.NotFound
		if errors.As(err, &notFound) {
			return err
		}

		return backoff.Permanent(err)
	}

	if err := backoff.Retry(operation, ctxBackoff); err != nil {
		return nil, mapWaitError(err, s.config.LockMaxWait)
	}

	return value, nil
}

//nolint:dupl
func (s *RedisCart) startLockHeartbeat(
	ctx context.Context,
	key, lockValue string,
	lockTTL, renewalInterval time.Duration,
) {
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

func getLockKey(key string) string {
	return key + lockKeySuffix
}

func generateLockValue() string {
	b := make([]byte, lockValueBytes)

	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}

func mapWaitError(err error, maxWait time.Duration) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("timeout waiting for cache population after %v", maxWait)
	case errors.Is(err, context.Canceled):
		return err
	default:
		return fmt.Errorf("waiting for cache population failed: %w", err)
	}
}
