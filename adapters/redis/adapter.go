// Package redis provides a Redis-backed implementation of cache.Cache.
//
// Import this package for its init() side effect to auto-register the adapter:
//
//	import _ "github.com/vinaycharlie01/nyro/adapters/redis"
//
// Then create a cache via the registry:
//
//	c, err := config.New(config.CacheRedis, &config.RedisConfig{Addr: "localhost:6379"})
//
// Or construct directly:
//
//	c, err := redis.New(config.RedisConfig{Addr: "localhost:6379"})
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	cache "github.com/vinaycharlie01/nyro"
	redisstore "github.com/vinaycharlie01/nyro/carts/redis"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

func init() {
	nyroconfig.Register(nyroconfig.CacheRedis, func(cfg nyroconfig.Config) (cache.Cache, error) {
		rc, ok := cfg.(*nyroconfig.RedisConfig)
		if !ok {
			return nil, fmt.Errorf("redis adapter: expected *config.RedisConfig, got %T", cfg)
		}

		return New(*rc)
	})
}

// Adapter implements cache.Cache backed by Redis.
type Adapter struct {
	store  *redisstore.RedisCart
	config nyroconfig.RedisConfig
}

const redisConnectTimeout = 5 * time.Second

// New creates a Redis-backed cache.Cache.
// It pings the server to verify connectivity before returning.
func New(cfg nyroconfig.RedisConfig) (*Adapter, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), redisConnectTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	var storeOpts []redisstore.RedisCartOption
	if cfg.LockTTL > 0 {
		storeOpts = append(storeOpts, redisstore.WithLockTTL(cfg.LockTTL))
	}

	if cfg.LockMaxWait > 0 {
		storeOpts = append(storeOpts, redisstore.WithLockMaxWait(cfg.LockMaxWait))
	}

	return &Adapter{
		store:  redisstore.NewRedis(client, storeOpts...),
		config: cfg,
	}, nil
}

func (a *Adapter) Get(ctx context.Context, key any) (any, error) {
	return a.store.Get(ctx, keyutil.ToString(key))
}

func (a *Adapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)

	return a.store.Set(ctx, keyutil.ToString(key), value, effectiveTTL(opts.Expiration, a.config.DefaultTTL))
}

func (a *Adapter) Delete(ctx context.Context, key any) error {
	return a.store.Delete(ctx, keyutil.ToString(key))
}

func (a *Adapter) Clear(ctx context.Context) error {
	return a.store.Clear(ctx)
}

func (a *Adapter) Exists(ctx context.Context, key any) (bool, error) {
	return a.store.Exists(ctx, keyutil.ToString(key))
}

const lockTTLC = 10 * time.Second

func (a *Adapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
	options := cache.ApplyOptions(opts...)
	ttl := effectiveTTL(options.Expiration, a.config.DefaultTTL)
	lockTTL := a.config.LockTTL
	if lockTTL == 0 {
		lockTTL = lockTTLC
	}

	return a.store.GetOrSetWithLock(ctx, keyutil.ToString(key), loader, ttl, lockTTL)
}

func (a *Adapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	if len(keys) == 0 {
		return make(map[any]any), nil
	}

	strKeys := make([]string, len(keys))
	keyMap := make(map[string]any, len(keys))

	for i, k := range keys {
		s := keyutil.ToString(k)
		strKeys[i] = s
		keyMap[s] = k
	}

	res, err := a.store.GetMulti(ctx, strKeys)
	if err != nil {
		return nil, err
	}

	out := make(map[any]any, len(res))
	for sk, v := range res {
		out[keyMap[sk]] = v
	}

	return out, nil
}

func (a *Adapter) SetMulti(ctx context.Context, items map[any]any, options ...cache.Option) error {
	if len(items) == 0 {
		return nil
	}

	opts := cache.ApplyOptions(options...)
	storeItems := make(map[string]any, len(items))

	for k, v := range items {
		storeItems[keyutil.ToString(k)] = v
	}

	return a.store.SetMulti(ctx, storeItems, effectiveTTL(opts.Expiration, a.config.DefaultTTL))
}

func (a *Adapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	strKeys := make([]string, len(keys))
	for i, k := range keys {
		strKeys[i] = keyutil.ToString(k)
	}

	return a.store.DeleteMulti(ctx, strKeys)
}

func (a *Adapter) HealthCheck(ctx context.Context) error {
	if a.store == nil {
		return errors.New("redis: store is nil")
	}

	return a.store.HealthCheck(ctx)
}

func (a *Adapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "redis",
		Connected: a.store.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	if a.store == nil {
		return nil
	}

	return a.store.Close()
}

const defaultTTL = 24 * time.Hour

// effectiveTTL returns the first non-zero duration, falling back to the default TTL.
func effectiveTTL(requested, configured time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}

	if configured > 0 {
		return configured
	}

	return defaultTTL
}
