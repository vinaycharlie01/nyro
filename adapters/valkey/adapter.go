// Package valkey provides a Valkey-backed implementation of cache.Cache.
//
// Import this package for its init() side effect to auto-register the adapter:
//
//	import _ "github.com/vinaycharlie01/nyro/adapters/valkey"
//
// Then create a cache via the registry:
//
//	c, err := config.New(config.CacheValkey, &config.ValkeyConfig{Addr: "localhost:6379"})
//
// Or construct directly:
//
//	c, err := valkey.New(config.ValkeyConfig{Addr: "localhost:6379"})
package valkey

import (
	"context"
	"errors"
	"fmt"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	cache "github.com/vinaycharlie01/nyro"
	valkeystorepkg "github.com/vinaycharlie01/nyro/carts/valkey"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

func init() {
	nyroconfig.Register(nyroconfig.CacheValkey, func(cfg nyroconfig.Config) (cache.Cache, error) {
		vc, ok := cfg.(*nyroconfig.ValkeyConfig)
		if !ok {
			return nil, fmt.Errorf("valkey adapter: expected *config.ValkeyConfig, got %T", cfg)
		}

		return New(*vc)
	})
}

// Adapter implements cache.Cache backed by Valkey.
type Adapter struct {
	store  *valkeystorepkg.ValkeyStore
	config nyroconfig.ValkeyConfig
}

const redisConnectTimeout = 5 * time.Second

// New creates a Valkey-backed cache.Cache.
// It pings the server to verify connectivity before returning.
func New(cfg nyroconfig.ValkeyConfig) (*Adapter, error) {
	client, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{cfg.Addr},
		Password:    cfg.Password,
		SelectDB:    cfg.DB,
	})
	if err != nil {
		return nil, fmt.Errorf("valkey: create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisConnectTimeout)
	defer cancel()

	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()

		return nil, fmt.Errorf("valkey: ping failed: %w", err)
	}

	var storeOpts []valkeystorepkg.ValkeyStoreOption
	if cfg.LockTTL > 0 {
		storeOpts = append(storeOpts, valkeystorepkg.WithLockTTL(cfg.LockTTL))
	}

	if cfg.LockMaxWait > 0 {
		storeOpts = append(storeOpts, valkeystorepkg.WithLockMaxWait(cfg.LockMaxWait))
	}

	return &Adapter{
		store:  valkeystorepkg.NewValkey(client, storeOpts...),
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

const (
	defaultLockTTL = 10 * time.Second
)

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
		lockTTL = defaultLockTTL
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
		return errors.New("valkey: store is nil")
	}

	return a.store.HealthCheck(ctx)
}

func (a *Adapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "valkey",
		Connected: a.store.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	if a.store == nil {
		return nil
	}

	return a.store.Close()
}

const (
	defaultTTLC = 24 * time.Hour
)

func effectiveTTL(requested, defaultTTL time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}

	if defaultTTL > 0 {
		return defaultTTL
	}

	return defaultTTLC
}
