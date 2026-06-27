// Deprecated: Use github.com/vinaycharlie01/nyro/adapters/valkey instead.
// This file is retained for backward compatibility and will be removed in v2.
package cache_adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	cache "github.com/vinaycharlie01/nyro"
	"github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
	valkeystorepkg "github.com/vinaycharlie01/nyro/stores/valkey"
)

// ValkeyAdapter implements cache.Cache using Valkey.
// Deprecated: Use adapters/valkey.Adapter instead.
type ValkeyAdapter struct {
	store  *valkeystorepkg.ValkeyStore
	cfg    config.ValkeyConfig
}

// NewValkeyAdapter creates a Valkey-backed cache adapter.
// Deprecated: Use adapters/valkey.New instead.
func NewValkeyAdapter(cfg config.ValkeyConfig) (*ValkeyAdapter, error) {
	client, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{cfg.Addr},
		Password:    cfg.Password,
		SelectDB:    cfg.DB,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	var storeOpts []valkeystorepkg.ValkeyStoreOption
	if cfg.LockTTL > 0 {
		storeOpts = append(storeOpts, valkeystorepkg.WithLockTTL(cfg.LockTTL))
	}

	if cfg.LockMaxWait > 0 {
		storeOpts = append(storeOpts, valkeystorepkg.WithLockMaxWait(cfg.LockMaxWait))
	}

	return &ValkeyAdapter{
		store: valkeystorepkg.NewValkey(client, storeOpts...),
		cfg:   cfg,
	}, nil
}

func (a *ValkeyAdapter) Get(ctx context.Context, key any) (any, error) {
	return a.store.Get(ctx, keyutil.ToString(key))
}

func (a *ValkeyAdapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
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

func (a *ValkeyAdapter) Delete(ctx context.Context, key any) error {
	return a.store.Delete(ctx, keyutil.ToString(key))
}

func (a *ValkeyAdapter) Clear(ctx context.Context) error {
	return a.store.Clear(ctx)
}

func (a *ValkeyAdapter) Exists(ctx context.Context, key any) (bool, error) {
	return a.store.Exists(ctx, keyutil.ToString(key))
}

func (a *ValkeyAdapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
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

func (a *ValkeyAdapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
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

func (a *ValkeyAdapter) SetMulti(ctx context.Context, items map[any]any, options ...cache.Option) error {
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

func (a *ValkeyAdapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	strKeys := make([]string, len(keys))
	for i, key := range keys {
		strKeys[i] = keyutil.ToString(key)
	}

	return a.store.DeleteMulti(ctx, strKeys)
}

func (a *ValkeyAdapter) HealthCheck(ctx context.Context) error {
	if a.store == nil {
		return errors.New("store is nil")
	}

	return a.store.HealthCheck(ctx)
}

func (a *ValkeyAdapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "valkey",
		Connected: a.store.HealthCheck(ctx) == nil,
	}, nil
}

func (a *ValkeyAdapter) Close() error {
	if a.store == nil {
		return nil
	}

	return a.store.Close()
}
