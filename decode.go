package cache

import (
	"encoding/json"
	"fmt"
)

// Decode converts a value returned by the Cache interface back to type T.
//
// Two cases are handled:
//   - Fast path: direct type assertion (in-memory cache hit or same-process value).
//   - JSON path: re-serialize then unmarshal to handle map[string]any or []any
//     produced when a Redis/Valkey backend deserializes JSON bytes.
//
// Example:
//
//	result, err := c.GetOrSet(ctx, "users", loader, cache.WithTTL(5*time.Minute))
//	if err != nil { return nil, err }
//	return cache.Decode[[]*domain.User](result)
func Decode[T any](result any) (T, error) {
	var target T

	if result == nil {
		return target, nil
	}

	if value, ok := result.(T); ok {
		return value, nil
	}

	data, err := json.Marshal(result)
	if err != nil {
		return target, fmt.Errorf("cache: marshal failed: %w", err)
	}

	if err := json.Unmarshal(data, &target); err != nil {
		return target, fmt.Errorf("cache: type mismatch in cached payload: %w", err)
	}

	return target, nil
}
