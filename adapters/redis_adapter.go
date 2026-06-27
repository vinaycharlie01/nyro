package cache_adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/vinaycharlie01/nyro/stores/redis"
)

func init() {
	// Register Redis cache factory
	Register(CacheRedis, func(cfg Config) (Cache, error) {
		redisConfig, ok := cfg.(*RedisConfig)
		if !ok {
			return nil, fmt.Errorf("invalid config type for Redis cache")
		}
		return NewRedisAdapter(*redisConfig)
	})
}

// RedisAdapter implements Cache interface using custom Redis store
type RedisAdapter struct {
	store  *redis.RedisStore
	config RedisConfig
}

// NewRedisAdapter creates a new Redis adapter using custom store implementation
func NewRedisAdapter(config RedisConfig) (*RedisAdapter, error) {
	// Create Redis client
	client := goredis.NewClient(&goredis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Create custom Redis store with configuration
	var storeOpts []redis.RedisStoreOption
	if config.LockTTL > 0 {
		storeOpts = append(storeOpts, redis.WithLockTTL(config.LockTTL))
	}
	if config.LockMaxWait > 0 {
		storeOpts = append(storeOpts, redis.WithLockMaxWait(config.LockMaxWait))
	}

	store := redis.NewRedis(client, storeOpts...)

	return &RedisAdapter{
		store:  store,
		config: config,
	}, nil
}

// Get retrieves a value from cache
func (a *RedisAdapter) Get(ctx context.Context, key any) (any, error) {
	strKey := keyToString(key)
	return a.store.Get(ctx, strKey)

}

// Set stores a value in cache
func (a *RedisAdapter) Set(ctx context.Context, key any, value any, options ...Option) error {
	strKey := keyToString(key)

	// Apply options to get expiration
	opts := ApplyOptions(options...)
	expiration := opts.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour // Default to 24 hours if not configured
	}

	return a.store.Set(ctx, strKey, value, expiration)
}

// Delete removes a key from cache
func (a *RedisAdapter) Delete(ctx context.Context, key any) error {
	strKey := keyToString(key)
	return a.store.Delete(ctx, strKey)
}

// Clear removes all keys from cache
func (a *RedisAdapter) Clear(ctx context.Context) error {
	return a.store.Clear(ctx)
}

// GetMulti retrieves multiple values from cache
func (a *RedisAdapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	if len(keys) == 0 {
		return make(map[any]any), nil
	}

	// Convert any keys to string keys
	strKeys := make([]string, len(keys))
	keyMap := make(map[string]any) // Map string key back to original key
	for i, key := range keys {
		strKey := keyToString(key)
		strKeys[i] = strKey
		keyMap[strKey] = key
	}

	// Get from store
	storeResult, err := a.store.GetMulti(ctx, strKeys)
	if err != nil {
		return nil, err
	}

	// Convert back to original key type
	result := make(map[any]any, len(storeResult))
	for strKey, value := range storeResult {
		originalKey := keyMap[strKey]
		result[originalKey] = value
	}

	return result, nil
}

// SetMulti stores multiple values in cache
func (a *RedisAdapter) SetMulti(ctx context.Context, items map[any]any, options ...Option) error {
	if len(items) == 0 {
		return nil
	}

	// Apply options to get expiration
	opts := ApplyOptions(options...)
	expiration := opts.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour
	}

	// Convert any keys to string keys
	storeItems := make(map[string]any, len(items))
	for key, value := range items {
		strKey := keyToString(key)
		storeItems[strKey] = value
	}

	return a.store.SetMulti(ctx, storeItems, expiration)
}

// DeleteMulti removes multiple keys from cache
func (a *RedisAdapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	// Convert any keys to string keys
	strKeys := make([]string, len(keys))
	for i, key := range keys {
		strKeys[i] = keyToString(key)
	}

	return a.store.DeleteMulti(ctx, strKeys)
}

// Exists checks if a key exists in cache
func (a *RedisAdapter) Exists(ctx context.Context, key any) (bool, error) {
	strKey := keyToString(key)
	return a.store.Exists(ctx, strKey)
}

// GetOrSet retrieves a value from cache, or sets it if not found
// Uses distributed locking to prevent cache stampede
func (a *RedisAdapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...Option) (any, error) {
	strKey := keyToString(key)

	// Apply options to get expiration
	options := ApplyOptions(opts...)
	expiration := options.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour
	}

	// Get lock TTL from config or use default
	lockTTL := a.config.LockTTL
	if lockTTL == 0 {
		lockTTL = 10 * time.Second // Default lock TTL
	}

	// Use distributed locking for cache stampede prevention
	return a.store.GetOrSetWithLock(ctx, strKey, loader, expiration, lockTTL)
}

// Type returns the cache backend type
func (a *RedisAdapter) Type() CacheType {
	return CacheRedis
}

// GetStats returns cache statistics
func (a *RedisAdapter) GetStats(ctx context.Context) (*Stats, error) {
	// Note: We need access to the underlying Redis client for stats
	// For now, return basic stats based on health check
	if err := a.store.HealthCheck(ctx); err != nil {
		return &Stats{
			Type:      string(CacheRedis),
			Connected: false,
		}, nil
	}

	return &Stats{
		Type:      string(CacheRedis),
		Connected: true,
	}, nil
}

// Close closes the Redis connection
func (a *RedisAdapter) Close() error {
	if a.store == nil {
		return nil
	}
	return a.store.Close()
}

// HealthCheck checks if Redis is accessible
func (a *RedisAdapter) HealthCheck(ctx context.Context) error {
	if a.store == nil {
		return errors.New("store is nil")
	}
	return a.store.HealthCheck(ctx)
}
