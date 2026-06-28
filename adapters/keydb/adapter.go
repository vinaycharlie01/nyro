// Package keydb provides a KeyDB-backed implementation of cache.Cache.
//
// KeyDB is a multithreaded, Redis-compatible in-memory data store. Because it
// speaks the Redis Serialization Protocol (RESP), this adapter reuses the Redis
// cart and the go-redis/v9 client with no protocol changes.
//
// Import this package for its init() side effect to auto-register the adapter:
//
//	import _ "github.com/vinaycharlie01/nyro/adapters/keydb"
//
// Then create a cache via the registry:
//
//	c, err := config.New(config.CacheKeyDB, &config.KeyDBConfig{Addr: "localhost:6379"})
//
// Or construct directly:
//
//	c, err := keydb.New(config.KeyDBConfig{Addr: "localhost:6379"})
package keydb

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	cache "github.com/vinaycharlie01/nyro"
	rediscart "github.com/vinaycharlie01/nyro/carts/redis"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

const (
	// DefaultAddr is the default KeyDB server address.
	DefaultAddr = "localhost:6379"
	// DefaultDB is the default KeyDB database number.
	DefaultDB = 0
	// DefaultTTL is the default time-to-live for cache entries.
	DefaultTTL = 24 * time.Hour
	// DefaultLockTTL is the default lock time-to-live for distributed locking.
	DefaultLockTTL = 10 * time.Second

	keydbConnectTimeout = 5 * time.Second
	keydbLockTTL        = 10 * time.Second
	keydbDefaultTTL     = 24 * time.Hour
)

func init() {
	nyroconfig.Register(nyroconfig.CacheKeyDB, func(cfg nyroconfig.Config) (cache.Cache, error) {
		kc, ok := cfg.(*nyroconfig.KeyDBConfig)
		if !ok {
			return nil, fmt.Errorf("keydb adapter: expected *config.KeyDBConfig, got %T", cfg)
		}

		return New(*kc)
	})
}

// Adapter implements cache.Cache backed by KeyDB.
type Adapter struct {
	cart   *rediscart.RedisCart
	config nyroconfig.KeyDBConfig
}

// New creates a KeyDB-backed cache.Cache.
// It pings the server to verify connectivity before returning.
func New(cfg nyroconfig.KeyDBConfig) (*Adapter, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), keydbConnectTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("keydb: ping failed: %w", err)
	}

	var cartOpts []rediscart.RedisCartOption
	if cfg.LockTTL > 0 {
		cartOpts = append(cartOpts, rediscart.WithLockTTL(cfg.LockTTL))
	}

	if cfg.LockMaxWait > 0 {
		cartOpts = append(cartOpts, rediscart.WithLockMaxWait(cfg.LockMaxWait))
	}

	return &Adapter{
		cart:   rediscart.NewRedis(client, cartOpts...),
		config: cfg,
	}, nil
}

func (a *Adapter) Get(ctx context.Context, key any) (any, error) {
	return a.cart.Get(ctx, keyutil.ToString(key))
}

func (a *Adapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)

	return a.cart.Set(ctx, keyutil.ToString(key), value, effectiveTTL(opts.Expiration, a.config.DefaultTTL))
}

func (a *Adapter) Delete(ctx context.Context, key any) error {
	return a.cart.Delete(ctx, keyutil.ToString(key))
}

func (a *Adapter) Clear(ctx context.Context) error {
	return a.cart.Clear(ctx)
}

func (a *Adapter) Exists(ctx context.Context, key any) (bool, error) {
	return a.cart.Exists(ctx, keyutil.ToString(key))
}

func (a *Adapter) GetOrSet(
	ctx context.Context,
	key any,
	loader func(context.Context) (any, error),
	opts ...cache.Option,
) (any, error) {
	options := cache.ApplyOptions(opts...)
	ttl := effectiveTTL(options.Expiration, a.config.DefaultTTL)

	lockTTL := a.config.LockTTL
	if lockTTL == 0 {
		lockTTL = keydbLockTTL
	}

	return a.cart.GetOrSetWithLock(ctx, keyutil.ToString(key), loader, ttl, lockTTL)
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

	res, err := a.cart.GetMulti(ctx, strKeys)
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
	cartItems := make(map[string]any, len(items))

	for k, v := range items {
		cartItems[keyutil.ToString(k)] = v
	}

	return a.cart.SetMulti(ctx, cartItems, effectiveTTL(opts.Expiration, a.config.DefaultTTL))
}

func (a *Adapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	strKeys := make([]string, len(keys))
	for i, k := range keys {
		strKeys[i] = keyutil.ToString(k)
	}

	return a.cart.DeleteMulti(ctx, strKeys)
}

func (a *Adapter) HealthCheck(ctx context.Context) error {
	if a.cart == nil {
		return errors.New("keydb: cart is nil")
	}

	return a.cart.HealthCheck(ctx)
}

func (a *Adapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "keydb",
		Connected: a.cart.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	if a.cart == nil {
		return nil
	}

	return a.cart.Close()
}

// effectiveTTL returns the first non-zero duration, falling back to the default TTL.
func effectiveTTL(requested, configured time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}

	if configured > 0 {
		return configured
	}

	return keydbDefaultTTL
}
