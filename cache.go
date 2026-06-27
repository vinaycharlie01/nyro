package cache

import (
	"context"
)

// Cache defines the interface for cache operations.
//
// Contract:
// - key should be a stable, deterministic value (typically string or primitive type)
// - keys used in SetMulti/GetMulti/DeleteMulti must be comparable when used as map keys
// - GetOrSet should provide single-loader semantics per key when backend supports locking

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 --fake-name MockCache . Cache
type Cache interface {
	// Core operations
	Get(ctx context.Context, key any) (any, error)
	Set(ctx context.Context, key any, value any, opts ...Option) error
	Delete(ctx context.Context, key any) error
	Clear(ctx context.Context) error
	Exists(ctx context.Context, key any) (bool, error)

	// Advanced operations
	// GetOrSet returns cached value when present, otherwise invokes loader and stores result.
	// Concurrent requests for the same key may be de-duplicated by the backend.
	// The loader function receives the context to support cancellation and timeouts.
	GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...Option) (any, error)
	GetMulti(ctx context.Context, keys []any) (map[any]any, error)
	SetMulti(ctx context.Context, items map[any]any, opts ...Option) error
	DeleteMulti(ctx context.Context, keys []any) error

	// Metadata
	HealthCheck(ctx context.Context) error
	GetStats(ctx context.Context) (*Stats, error)
	Close() error
}
