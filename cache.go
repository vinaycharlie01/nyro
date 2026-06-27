package cache

import "context"

// Stats holds runtime statistics for a cache backend.
type Stats struct {
	Type      string `json:"type"`
	Connected bool   `json:"connected"`
	Info      string `json:"info"`
}

// Cache defines the port for all cache operations.
//
// Contract:
//   - key must be a stable, deterministic, comparable value.
//   - keys used in SetMulti/GetMulti/DeleteMulti must be comparable.
//   - GetOrSet provides single-loader semantics; backends should de-duplicate
//     concurrent calls for the same key via distributed locking or singleflight.
//   - All operations accept a context for cancellation and deadline propagation.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 --fake-name MockCache . Cache
type Cache interface {
	// Core CRUD operations
	Get(ctx context.Context, key any) (any, error)
	Set(ctx context.Context, key any, value any, opts ...Option) error
	Delete(ctx context.Context, key any) error
	Clear(ctx context.Context) error
	Exists(ctx context.Context, key any) (bool, error)

	// Advanced operations
	// GetOrSet returns the cached value when present; otherwise invokes loader,
	// stores the result, and returns it. Concurrent requests for the same key
	// should be de-duplicated by the backend when possible.
	GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...Option) (any, error)
	GetMulti(ctx context.Context, keys []any) (map[any]any, error)
	SetMulti(ctx context.Context, items map[any]any, opts ...Option) error
	DeleteMulti(ctx context.Context, keys []any) error

	// Metadata and lifecycle
	HealthCheck(ctx context.Context) error
	GetStats(ctx context.Context) (*Stats, error)
	Close() error
}
