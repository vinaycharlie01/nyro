package cache

import (
	"context"
)

// TypedCache provides a type-safe wrapper around the Cache interface using Go generics.
// It eliminates the need for manual type assertions and provides compile-time type safety.
//
// Example usage:
//
//	// Create a typed cache for User pointers
//	userCache := cache.NewTypedCache[*domain.User](baseCache)
//
//	// Type-safe operations
//	user, err := userCache.Get(ctx, "user:123")
//	err = userCache.Set(ctx, "user:123", &domain.User{...})
//
//	// GetOrSet with type-safe loader
//	user, err := userCache.GetOrSet(ctx, "user:123", func(ctx context.Context) (*domain.User, error) {
//	    return repo.FindByID(ctx, 123)
//	})
//
// TypedCache[T] wraps the underlying Cache interface and provides type-safe methods.
type TypedCache[T any] struct {
	cache Cache
}

// NewTypedCache creates a new type-safe cache wrapper.
// The generic type parameter T specifies the type of values stored in the cache.
func NewTypedCache[T any](cache Cache) *TypedCache[T] {
	return &TypedCache[T]{cache: cache}
}

// Get retrieves a value from cache with automatic type conversion.
// Returns the typed value and any error encountered.
func (tc *TypedCache[T]) Get(ctx context.Context, key any) (T, error) {
	result, err := tc.cache.Get(ctx, key)
	if err != nil {
		return zeroValue[T](), err
	}
	return Decode[T](result)
}

// Set stores a typed value in cache.
func (tc *TypedCache[T]) Set(ctx context.Context, key any, value T, opts ...Option) error {
	return tc.cache.Set(ctx, key, value, opts...)
}

// Delete removes a key from cache.
func (tc *TypedCache[T]) Delete(ctx context.Context, key any) error {
	return tc.cache.Delete(ctx, key)
}

// Clear removes all keys from cache.
func (tc *TypedCache[T]) Clear(ctx context.Context) error {
	return tc.cache.Clear(ctx)
}

// Exists checks if a key exists in cache.
func (tc *TypedCache[T]) Exists(ctx context.Context, key any) (bool, error) {
	return tc.cache.Exists(ctx, key)
}

// GetOrSet retrieves a value from cache, or loads and stores it if not found.
// The loader function is type-safe and returns T directly.
func (tc *TypedCache[T]) GetOrSet(ctx context.Context, key any, loader func(context.Context) (T, error), opts ...Option) (T, error) {
	// Wrap the typed loader to match the Cache interface
	wrappedLoader := func(ctx context.Context) (any, error) {
		return loader(ctx)
	}

	result, err := tc.cache.GetOrSet(ctx, key, wrappedLoader, opts...)
	if err != nil {
		return zeroValue[T](), err
	}

	return Decode[T](result)
}

// GetMulti retrieves multiple values from cache with type conversion.
// Returns a map of keys to typed values.
func (tc *TypedCache[T]) GetMulti(ctx context.Context, keys []any) (map[any]T, error) {
	results, err := tc.cache.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	typedResults := make(map[any]T, len(results))
	for key, value := range results {
		decoded, err := Decode[T](value)
		if err != nil {
			return nil, err
		}
		typedResults[key] = decoded
	}

	return typedResults, nil
}

// SetMulti stores multiple typed values in cache.
func (tc *TypedCache[T]) SetMulti(ctx context.Context, items map[any]T, opts ...Option) error {
	// Convert typed map to any map
	anyItems := make(map[any]any, len(items))
	for key, value := range items {
		anyItems[key] = value
	}
	return tc.cache.SetMulti(ctx, anyItems, opts...)
}

// DeleteMulti removes multiple keys from cache.
func (tc *TypedCache[T]) DeleteMulti(ctx context.Context, keys []any) error {
	return tc.cache.DeleteMulti(ctx, keys)
}

// HealthCheck checks if the underlying cache is accessible.
func (tc *TypedCache[T]) HealthCheck(ctx context.Context) error {
	return tc.cache.HealthCheck(ctx)
}

// GetStats returns cache statistics.
func (tc *TypedCache[T]) GetStats(ctx context.Context) (*Stats, error) {
	return tc.cache.GetStats(ctx)
}

// Close closes the underlying cache connection.
func (tc *TypedCache[T]) Close() error {
	return tc.cache.Close()
}

// Unwrap returns the underlying Cache interface.
// This is useful when you need to pass the cache to code that expects the base interface.
func (tc *TypedCache[T]) Unwrap() Cache {
	return tc.cache
}
