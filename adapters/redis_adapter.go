// Deprecated: Use github.com/vinaycharlie01/nyro/adapters/redis instead.
// This file is retained for backward compatibility and will be removed in v2.
package cache_adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	cache "github.com/vinaycharlie01/nyro"
	"github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
	redisstore "github.com/vinaycharlie01/nyro/stores/redis"
)

// RedisAdapter implements cache.Cache using Redis.
// Deprecated: Use adapters/redis.Adapter instead.
type RedisAdapter struct {
	store  *redisstore.RedisStore
	cfg    config.RedisConfig
}

// NewRedisAdapter creates a Redis-backed cache adapter.
// Deprecated: Use adapters/redis.New instead.
func NewRedisAdapter(cfg config.RedisConfig) (*RedisAdapter, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	var storeOpts []redisstore.RedisStoreOption
	if cfg.LockTTL > 0 {
		storeOpts = append(storeOpts, redisstore.WithLockTTL(cfg.LockTTL))
	}

	if cfg.LockMaxWait > 0 {
		storeOpts = append(storeOpts, redisstore.WithLockMaxWait(cfg.LockMaxWait))
	}

	return &RedisAdapter{
		store: redisstore.NewRedis(client, storeOpts...),
		cfg:   cfg,
	}, nil
}

func (a *RedisAdapter) Get(ctx context.Context, key any) (any, error) {
	return a.store.Get(ctx, keyutil.ToString(key))
}

func (a *RedisAdapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration
	if ttl == 0 {
		ttl = a.cfg.DefaultTTL
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	return a.store.Set(ctx, keyutil.ToString(key), value, ttl)
}

func (a *RedisAdapter) Delete(ctx context.Context, key any) error {
	return a.store.Delete(ctx, keyutil.ToString(key))
}

func (a *RedisAdapter) Clear(ctx context.Context) error {
	return a.store.Clear(ctx)
}

func (a *RedisAdapter) Exists(ctx context.Context, key any) (bool, error) {
	return a.store.Exists(ctx, keyutil.ToString(key))
}

func (a *RedisAdapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
	options := cache.ApplyOptions(opts...)
	ttl := options.Expiration
	if ttl == 0 {
		ttl = a.cfg.DefaultTTL
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	lockTTL := a.cfg.LockTTL
	if lockTTL == 0 {
		lockTTL = 10 * time.Second
	}

	return a.store.GetOrSetWithLock(ctx, keyutil.ToString(key), loader, ttl, lockTTL)
}

func (a *RedisAdapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	if len(keys) == 0 {
		return make(map[any]any), nil
	}

	strKeys := make([]string, len(keys))
	keyMap := make(map[string]any, len(keys))

	for i, key := range keys {
		s := keyutil.ToString(key)
		strKeys[i] = s
		keyMap[s] = key
	}

	storeResult, err := a.store.GetMulti(ctx, strKeys)
	if err != nil {
		return nil, err
	}

	result := make(map[any]any, len(storeResult))
	for strKey, value := range storeResult {
		result[keyMap[strKey]] = value
	}

	return result, nil
}

func (a *RedisAdapter) SetMulti(ctx context.Context, items map[any]any, options ...cache.Option) error {
	if len(items) == 0 {
		return nil
	}

	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration
	if ttl == 0 {
		ttl = a.cfg.DefaultTTL
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	storeItems := make(map[string]any, len(items))
	for key, value := range items {
		storeItems[keyutil.ToString(key)] = value
	}

	return a.store.SetMulti(ctx, storeItems, ttl)
}

func (a *RedisAdapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	strKeys := make([]string, len(keys))
	for i, key := range keys {
		strKeys[i] = keyutil.ToString(key)
	}

	return a.store.DeleteMulti(ctx, strKeys)
}

func (a *RedisAdapter) HealthCheck(ctx context.Context) error {
	if a.store == nil {
		return errors.New("store is nil")
	}

	return a.store.HealthCheck(ctx)
}

func (a *RedisAdapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "redis",
		Connected: a.store.HealthCheck(ctx) == nil,
	}, nil
}

func (a *RedisAdapter) Close() error {
	if a.store == nil {
		return nil
	}

	return a.store.Close()
}
