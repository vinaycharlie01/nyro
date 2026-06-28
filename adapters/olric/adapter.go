// Package olric provides an Olric-backed implementation of cache.Cache.
package olric

import (
	"context"
	"time"

	cache "github.com/vinaycharlie01/nyro"
	olriccart "github.com/vinaycharlie01/nyro/carts/olric"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

const (
	// DefaultDMapName is the default DMap name for cache entries.
	DefaultDMapName = "cache"
	// DefaultTTL is the default time-to-live for cache entries.
	DefaultTTL = 24 * time.Hour
	// DefaultLockTTL is the default lock time-to-live for distributed locking.
	DefaultLockTTL = 10 * time.Second
)

// Adapter implements cache.Cache backed by Olric.
type Adapter struct {
	cart *olriccart.OlricCart
	ttl  time.Duration
}

// New creates an Olric-backed cache.Cache.
func New(client olriccart.OlricClientInterface, defaultTTL time.Duration, opts ...olriccart.OlricCartOption) (*Adapter, error) {
	if defaultTTL == 0 {
		defaultTTL = DefaultTTL
	}

	cart, err := olriccart.NewOlric(client, opts...)
	if err != nil {
		return nil, err
	}

	return &Adapter{
		cart: cart,
		ttl:  defaultTTL,
	}, nil
}

func (a *Adapter) Get(ctx context.Context, key any) (any, error) {
	return a.cart.Get(ctx, keyutil.ToString(key))
}

func (a *Adapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration
	if ttl == 0 {
		ttl = a.ttl
	}

	return a.cart.Set(ctx, keyutil.ToString(key), value, ttl)
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

func (a *Adapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
	options := cache.ApplyOptions(opts...)
	ttl := options.Expiration
	if ttl == 0 {
		ttl = a.ttl
	}

	lockTTL := DefaultLockTTL

	return a.cart.GetOrSetWithLock(ctx, keyutil.ToString(key), loader, ttl, lockTTL)
}

func (a *Adapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
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
	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration
	if ttl == 0 {
		ttl = a.ttl
	}

	storeItems := make(map[string]any, len(items))
	for k, v := range items {
		storeItems[keyutil.ToString(k)] = v
	}

	return a.cart.SetMulti(ctx, storeItems, ttl)
}

func (a *Adapter) DeleteMulti(ctx context.Context, keys []any) error {
	strKeys := make([]string, len(keys))
	for i, k := range keys {
		strKeys[i] = keyutil.ToString(k)
	}

	return a.cart.DeleteMulti(ctx, strKeys)
}

func (a *Adapter) HealthCheck(ctx context.Context) error {
	return a.cart.HealthCheck(ctx)
}

func (a *Adapter) GetStats(ctx context.Context) (*cache.Stats, error) {
	return &cache.Stats{
		Type:      "olric",
		Connected: a.cart.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	return a.cart.Close()
}
