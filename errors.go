package cache

import "errors"

// ErrNotFound is returned when a requested key does not exist in the cache.
var ErrNotFound = errors.New("cache: key not found")

// ErrBackendUnavailable is returned when the cache backend cannot be reached.
var ErrBackendUnavailable = errors.New("cache: backend unavailable")
