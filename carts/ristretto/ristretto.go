// Package ristretto provides a high-performance, bounded, concurrent in-memory
// cache backend using DGraph's Ristretto (TinyLFU eviction).
package ristretto

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	cart "github.com/vinaycharlie01/nyro/carts"
	"golang.org/x/sync/singleflight"
)

const (
	// RistrettoType is the identifier for the Ristretto cart backend.
	RistrettoType = "ristretto"
)

// RistrettoCartConfig holds configuration for the Ristretto cart backend.
type RistrettoCartConfig struct {
	// NumCounters is the number of access counters to keep for admission.
	// Default: 10x estimated max unique keys.
	NumCounters int64
	// MaxCost is the maximum cost of the cache (roughly max memory in bytes).
	// Default: 100 MB.
	MaxCost int64
	// BufferItems is the number of items per internal buffer. Default: 64.
	BufferItems int64
	// DefaultTTL is used when no per-entry TTL is provided. Zero means no expiry.
	DefaultTTL time.Duration
}

// DefaultRistrettoCartConfig returns sensible defaults for a Ristretto cart.
func DefaultRistrettoCartConfig() *RistrettoCartConfig {
	return &RistrettoCartConfig{
		NumCounters: 1e7,       // 10M counters → ~1M unique keys
		MaxCost:     100 << 20, // 100 MB
		BufferItems: 64,
	}
}

// RistrettoCart implements cart.Cart backed by Ristretto.
type RistrettoCart struct {
	cache  *ristretto.Cache[string, any]
	config *RistrettoCartConfig
	mu     sync.RWMutex
	closed bool
	group  singleflight.Group
}

// NewRistretto creates a new RistrettoCart instance.
func NewRistretto(cfg *RistrettoCartConfig) (*RistrettoCart, error) {
	if cfg == nil {
		cfg = DefaultRistrettoCartConfig()
	}

	c, err := ristretto.NewCache[string, any](&ristretto.Config[string, any]{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: cfg.BufferItems,
	})
	if err != nil {
		return nil, fmt.Errorf("ristretto: new cache: %w", err)
	}

	return &RistrettoCart{
		cache:  c,
		config: cfg,
	}, nil
}

func (s *RistrettoCart) Get(_ context.Context, key string) (any, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("ristretto: cart closed")
	}

	val, found := s.cache.Get(key)
	if !found {
		return nil, cart.NotFoundWithCause(fmt.Errorf("key %q not found in ristretto", key))
	}
	return val, nil
}

func (s *RistrettoCart) Set(_ context.Context, key string, value any, expiration time.Duration) error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return fmt.Errorf("ristretto: cart closed")
	}

	var ttl time.Duration
	if expiration > 0 {
		ttl = expiration
	} else if s.config.DefaultTTL > 0 {
		ttl = s.config.DefaultTTL
	}

	// Ristretto uses cost; we approximate cost as 1 per entry.
	if !s.cache.SetWithTTL(key, value, 1, ttl) {
		return fmt.Errorf("ristretto: set dropped for key %q", key)
	}
	return nil
}

func (s *RistrettoCart) Delete(_ context.Context, key string) error {
	s.cache.Del(key)
	return nil
}

func (s *RistrettoCart) Exists(_ context.Context, key string) (bool, error) {
	_, found := s.cache.Get(key)
	return found, nil
}

func (s *RistrettoCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	result := make(map[string]any, len(keys))
	for _, k := range keys {
		if v, err := s.Get(ctx, k); err == nil {
			result[k] = v
		}
	}
	return result, nil
}

func (s *RistrettoCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	for k, v := range items {
		if err := s.Set(ctx, k, v, expiration); err != nil {
			return err
		}
	}
	return nil
}

func (s *RistrettoCart) DeleteMulti(ctx context.Context, keys []string) error {
	for _, k := range keys {
		_ = s.Delete(ctx, k)
	}
	return nil
}

func (s *RistrettoCart) Clear(_ context.Context) error {
	s.cache.Clear()
	return nil
}

func (s *RistrettoCart) GetType() string { return RistrettoType }

func (s *RistrettoCart) HealthCheck(_ context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("ristretto: cart closed")
	}
	return nil
}

func (s *RistrettoCart) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.cache.Close()
	}
	return nil
}

// GetOrSetWithLock retrieves a cached value or populates it with singleflight
// stampede protection (in-process only; no distributed locking).
func (s *RistrettoCart) GetOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	if v, err := s.Get(ctx, key); err == nil {
		return v, nil
	}

	result, err, _ := s.group.Do(key, func() (any, error) {
		// Double-check
		if v, err := s.Get(ctx, key); err == nil {
			return v, nil
		}

		val, loaderErr := loader(ctx)
		if loaderErr != nil {
			return nil, fmt.Errorf("ristretto: loader failed: %w", loaderErr)
		}

		ttl := expiration
		if ttl == 0 {
			ttl = s.config.DefaultTTL
		}
		if setErr := s.Set(ctx, key, val, ttl); setErr != nil {
			return nil, setErr
		}

		return val, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
