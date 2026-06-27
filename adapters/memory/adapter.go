// Package memory provides a process-local, in-memory implementation of cache.Cache.
// It is suitable for testing, single-instance services, and L1 caching layers.
//
// Import this package for its init() side effect to auto-register the adapter:
//
//	import _ "github.com/vinaycharlie01/nyro/adapters/memory"
//
// Then create a cache via the registry:
//
//	c, err := config.New(config.CacheMemory, &config.MemoryConfig{DefaultTTL: time.Minute})
//
// Or construct directly:
//
//	c := memory.New(config.MemoryConfig{DefaultTTL: time.Minute})
package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	cache "github.com/vinaycharlie01/nyro"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
)

func init() {
	nyroconfig.Register(nyroconfig.CacheMemory, func(cfg nyroconfig.Config) (cache.Cache, error) {
		mc, ok := cfg.(*nyroconfig.MemoryConfig)
		if !ok {
			return nil, fmt.Errorf("memory adapter: expected *config.MemoryConfig, got %T", cfg)
		}

		return New(*mc), nil
	})
}

type entry struct {
	value     any
	expiresAt time.Time // zero → no expiry
}

func (e *entry) expired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

// Adapter is a thread-safe, process-local cache.
type Adapter struct {
	mu        sync.RWMutex
	data      map[string]*entry
	config    nyroconfig.MemoryConfig
	group     singleflight.Group
	closeCh   chan struct{}
	closeOnce sync.Once
}

// New creates a new in-memory cache.Cache.
func New(cfg nyroconfig.MemoryConfig) *Adapter {
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = time.Minute
	}

	a := &Adapter{
		data:    make(map[string]*entry),
		config:  cfg,
		closeCh: make(chan struct{}),
	}

	go a.runGC()

	return a
}

func (a *Adapter) runGC() {
	ticker := time.NewTicker(a.config.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.evictExpired()
		case <-a.closeCh:
			return
		}
	}
}

func (a *Adapter) evictExpired() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, e := range a.data {
		if e.expired() {
			delete(a.data, k)
		}
	}
}

func (a *Adapter) toKey(key any) string {
	if s, ok := key.(string); ok {
		return s
	}

	return fmt.Sprintf("%v", key)
}

func (a *Adapter) Get(_ context.Context, key any) (any, error) {
	a.mu.RLock()
	e, ok := a.data[a.toKey(key)]
	a.mu.RUnlock()

	if !ok || e.expired() {
		return nil, cache.ErrNotFound
	}

	return e.value, nil
}

func (a *Adapter) Set(_ context.Context, key any, value any, options ...cache.Option) error {
	opts := cache.ApplyOptions(options...)
	ttl := opts.Expiration

	if ttl == 0 {
		ttl = a.config.DefaultTTL
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	a.mu.Lock()
	a.data[a.toKey(key)] = &entry{value: value, expiresAt: expiresAt}
	a.mu.Unlock()

	return nil
}

func (a *Adapter) Delete(_ context.Context, key any) error {
	a.mu.Lock()
	delete(a.data, a.toKey(key))
	a.mu.Unlock()

	return nil
}

func (a *Adapter) Clear(_ context.Context) error {
	a.mu.Lock()
	a.data = make(map[string]*entry)
	a.mu.Unlock()

	return nil
}

func (a *Adapter) Exists(ctx context.Context, key any) (bool, error) {
	_, err := a.Get(ctx, key)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, cache.ErrNotFound) {
		return false, nil
	}

	return false, err
}

func (a *Adapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
	sk := a.toKey(key)

	if v, err := a.Get(ctx, key); err == nil {
		return v, nil
	}

	result, err, _ := a.group.Do(sk, func() (any, error) {
		// Double-check after acquiring the singleflight slot.
		if v, err := a.Get(ctx, key); err == nil {
			return v, nil
		}

		val, loaderErr := loader(ctx)
		if loaderErr != nil {
			return nil, fmt.Errorf("memory: loader failed: %w", loaderErr)
		}

		_ = a.Set(ctx, key, val, opts...)

		return val, nil
	})

	return result, err
}

func (a *Adapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	out := make(map[any]any, len(keys))

	for _, k := range keys {
		if v, err := a.Get(ctx, k); err == nil {
			out[k] = v
		}
	}

	return out, nil
}

func (a *Adapter) SetMulti(ctx context.Context, items map[any]any, opts ...cache.Option) error {
	for k, v := range items {
		if err := a.Set(ctx, k, v, opts...); err != nil {
			return err
		}
	}

	return nil
}

func (a *Adapter) DeleteMulti(ctx context.Context, keys []any) error {
	for _, k := range keys {
		if err := a.Delete(ctx, k); err != nil {
			return err
		}
	}

	return nil
}

func (a *Adapter) HealthCheck(_ context.Context) error {
	return nil
}

func (a *Adapter) GetStats(_ context.Context) (*cache.Stats, error) {
	a.mu.RLock()
	size := len(a.data)
	a.mu.RUnlock()

	return &cache.Stats{
		Type:      "memory",
		Connected: true,
		Info:      fmt.Sprintf("entries=%d", size),
	}, nil
}

func (a *Adapter) Close() error {
	a.closeOnce.Do(func() { close(a.closeCh) })

	return nil
}
