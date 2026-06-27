package store

import (
	"context"
	"time"
)

// Store defines the interface for cache store operations
// This abstraction allows multiple backend implementations (Redis, Memcached, etc.)
type Store interface {
	// Get retrieves a value from the store
	Get(ctx context.Context, key string) (any, error)

	// Set stores a value with optional expiration
	Set(ctx context.Context, key string, value any, expiration time.Duration) error

	// Delete removes a key from the store
	Delete(ctx context.Context, key string) error

	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (bool, error)

	// GetMulti retrieves multiple keys at once
	GetMulti(ctx context.Context, keys []string) (map[string]any, error)

	// SetMulti sets multiple keys at once
	SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error

	// DeleteMulti removes multiple keys at once
	DeleteMulti(ctx context.Context, keys []string) error

	// Clear flushes all keys from the store
	Clear(ctx context.Context) error

	// GetType returns the store type identifier
	GetType() string

	// HealthCheck verifies the store is accessible
	HealthCheck(ctx context.Context) error

	// Close closes the store connection
	Close() error
}

// DistributedLocker defines interface for distributed lock operations
// Used to prevent cache stampede in distributed environments
type DistributedLocker interface {
	// AcquireLock attempts to acquire a distributed lock
	// Returns lockValue (for safe release), acquired (true if lock obtained), and error
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error)

	// ReleaseLock releases a distributed lock safely (with ownership check)
	// Only releases if lockValue matches
	ReleaseLock(ctx context.Context, key string, lockValue string) error

	// ExtendLock extends the TTL of an existing lock with ownership check
	// Returns true if lock was extended, false if lock no longer owned
	ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error)
}

// StoreWithLocking combines Store and DistributedLocker interfaces
// Implementations that support distributed locking should implement this
type StoreWithLocking interface {
	Store
	DistributedLocker
}

// Options holds configuration for store operations
type Options struct {
	Expiration time.Duration
	Tags       []string
}

// Option is a functional option for configuring store operations
type Option func(*Options)

// WithExpiration sets the expiration duration
func WithExpiration(d time.Duration) Option {
	return func(o *Options) {
		o.Expiration = d
	}
}

// WithTags sets tags for group invalidation
func WithTags(tags ...string) Option {
	return func(o *Options) {
		o.Tags = append(o.Tags, tags...)
	}
}

// ApplyOptions applies functional options and returns Options
func ApplyOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
