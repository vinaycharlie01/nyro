package cache

import (
	"encoding/json"
	"fmt"
)

// Decode converts cached values back to their expected type.
//
// It handles:
//   - Direct type assertions (cache miss / in-memory value)
//   - JSON-decoded values (cache hit from Redis)
//
// This is useful when implementing the cache-aside pattern with the Cache interface.
//
// Examples:
//
//	// Decode slice of pointers
//	users, err := cache.Decode[[]*domain.User](result)
//
//	// Decode single pointer
//	role, err := cache.Decode[*domain.Role](result)
//
//	// Decode map
//	params, err := cache.Decode[map[string][]*domain.SchemeParameter](result)
//
// Usage with GetOrSet:
//
//	result, err := c.GetOrSet(ctx, "users:active",
//	    func() (any, error) {
//	        return repo.FindAllActive(ctx)
//	    },
//	    WithTTL(5*time.Minute),
//	)
//	if err != nil {
//	    return nil, err
//	}
//	return cache.Decode[[]*domain.User](result)
func Decode[T any](result any) (T, error) {
	var target T

	if result == nil {
		return target, nil
	}

	// Fast path: direct type assertion
	// This succeeds when the value comes from in-memory cache or cache miss
	if value, ok := result.(T); ok {
		return value, nil
	}

	// Fallback: convert generic cache payloads
	// Redis returns values as map[string]any or []any after JSON decode
	data, err := json.Marshal(result)
	if err != nil {
		return target, fmt.Errorf("cache: failed to marshal cached value: %w", err)
	}

	if err := json.Unmarshal(data, &target); err != nil {
		return target, fmt.Errorf("cache: invalid cached payload: %w", err)
	}

	return target, nil
}
