// Package pebble provides a Pebble-backed implementation of cache.Cache.
package pebble

import (
	"context"
	"errors"
	"time"

	cache "github.com/vinaycharlie01/nyro"
	cart "github.com/vinaycharlie01/nyro/carts"
	pebblecart "github.com/vinaycharlie01/nyro/carts/pebble"
	"github.com/vinaycharlie01/nyro/internal/keyutil"
)

const (
	// DefaultTTL is the default time-to-live for cache entries.
	DefaultTTL = 24 * time.Hour
)

// Adapter implements cache.Cache backed by Pebble.
type Adapter struct {
	cart *pebblecart.PebbleCart
	ttl  time.Duration
}

// New creates a Pebble-backed cache.Cache.
func New(cart *pebblecart.PebbleCart, defaultTTL time.Duration) *Adapter {
	if defaultTTL == 0 {
		defaultTTL = DefaultTTL
	}

	return &Adapter{
		cart: cart,
		ttl:  defaultTTL,
	}
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
	// Try to get the value first
	value, err := a.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	// If not found, load and set
	var notFound *cart.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	// Load the value
	value, err = loader(ctx)
	if err != nil {
		return nil, err
	}

	// Store the value
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
		Type:      "pebble",
		Connected: a.cart.HealthCheck(ctx) == nil,
	}, nil
}

func (a *Adapter) Close() error {
	return a.cart.Close()
}
