// Package ristretto provides a Ristretto-backed implementation of cache.Cache.
// Ristretto is a high-performance, concurrent, bounded in-memory cache from DGraph
// that uses TinyLFU admission and eviction policies.
//
// Import this package for its init() side effect to auto-register the adapter:
//
//	import _ "github.com/vinaycharlie01/nyro/adapters/ristretto"
//
// Then create a cache via the registry:
//
//	c, err := config.New(config.CacheRistretto, &config.RistrettoConfig{DefaultTTL: time.Minute})
//
// Or construct directly:
//
//	c, err := ristretto.New(config.RistrettoConfig{DefaultTTL: time.Minute})
package ristretto

import (
	"context"
	"errors"
	"fmt"
	"time"

	cache "github.com/vinaycharlie01/nyro"
	cart "github.com/vinaycharlie01/nyro/carts"
	ristrettocart "github.com/vinaycharlie01/nyro/carts/ristretto"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

const (
	// DefaultTTL is the default time-to-live for cache entries.
	DefaultTTL = 24 * time.Hour
)

func init() {
	nyroconfig.Register(nyroconfig.CacheRistretto, func(cfg nyroconfig.Config) (cache.Cache, error) {
		rc, ok := cfg.(*nyroconfig.RistrettoConfig)
		if !ok {
			return nil, fmt.Errorf("ristretto adapter: expected *config.RistrettoConfig, got %T", cfg)
		}

		return New(*rc)
	})
}

// Adapter implements cache.Cache backed by Ristretto.
type Adapter struct {
	cart   *ristrettocart.RistrettoCart
	config nyroconfig.RistrettoConfig
}

// New creates a Ristretto-backed cache.Cache.
func New(cfg nyroconfig.RistrettoConfig) (*Adapter, error) {
	var cartCfg ristrettocart.RistrettoCartConfig
	if cfg.NumCounters > 0 {
		cartCfg.NumCounters = cfg.NumCounters
	}
	if cfg.MaxCost > 0 {
		cartCfg.MaxCost = cfg.MaxCost
	}
	if cfg.BufferItems > 0 {
		cartCfg.BufferItems = cfg.BufferItems
	}
	cartCfg.DefaultTTL = cfg.DefaultTTL

	if cfg.DefaultTTL == 0 {
		cartCfg.DefaultTTL = DefaultTTL
	}

	c, err := ristrettocart.NewRistretto(&cartCfg)
	if err != nil {
		return nil, fmt.Errorf("ristretto: create cart: %w", err)
	}

	return &Adapter{
		cart:   c,
		config: cfg,
	}, nil
}

func (a *Adapter) Get(ctx context.Context, key any) (any, error) {
	return a.cart.Get(ctx, keyutil.ToString(key))
}

func (a *Adapter) Set(ctx context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration
	if ttl == 0 {
		ttl = a.config.DefaultTTL
	}
	if ttl == 0 {
		ttl = DefaultTTL
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
	value, err := a.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFound *cart.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	value, err = loader(ctx)
	if err != nil {
		return nil, err
	}

	if err := a.Set(ctx, key, value, opts...); err != nil {
		return nil, err
	}

	return value, nil
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
		ttl = a.config.DefaultTTL
	}
	if ttl == 0 {
		ttl = DefaultTTL
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
		Type:      "ristretto",
		Connected: a.cart.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	return a.cart.Close()
}
