package cart

import (
	"context"
	"time"
)

// Cart defines the interface for cart backend operations.
// Implementations include Redis, Valkey, and in-memory backends.
type Cart interface {
	// Get retrieves a value from the cart.
	Get(ctx context.Context, key string) (any, error)

	// Set stores a value with optional expiration.
	Set(ctx context.Context, key string, value any, expiration time.Duration) error

	// Delete removes a key from the cart.
	Delete(ctx context.Context, key string) error

	// Exists checks if a key exists in the cart.
	Exists(ctx context.Context, key string) (bool, error)

	// GetMulti retrieves multiple keys in a single batch call.
	GetMulti(ctx context.Context, keys []string) (map[string]any, error)

	// SetMulti stores multiple key-value pairs with the same expiration.
	SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error

	// DeleteMulti removes multiple keys at once.
	DeleteMulti(ctx context.Context, keys []string) error

	// Clear removes all keys from the cart.
	Clear(ctx context.Context) error

	// GetType returns the cart backend type identifier.
	GetType() string

	// HealthCheck verifies the cart backend is accessible.
	HealthCheck(ctx context.Context) error

	// Close closes the cart backend connection.
	Close() error
}

// DistributedLocker defines the interface for distributed lock operations.
// Used to prevent cache stampede in distributed environments.
type DistributedLocker interface {
	// AcquireLock attempts to acquire a distributed lock for the given key.
	// Returns lockValue (for safe release), acquired (true if obtained), and any error.
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error)

	// ReleaseLock releases a distributed lock safely via an ownership check.
	// The lock is only released when the provided lockValue matches the stored value.
	ReleaseLock(ctx context.Context, key string, lockValue string) error

	// ExtendLock extends the TTL of an existing lock via an ownership check.
	// Returns true if the lock was extended, false if the lock is no longer owned.
	ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error)
}

// CartWithLocking combines Cart and DistributedLocker.
// Backends that support distributed locking should implement this interface.
type CartWithLocking interface {
	Cart
	DistributedLocker
}

// Options holds configuration for cart operations.
type Options struct {
	Expiration time.Duration
	Tags       []string
}

// Option is a functional option for configuring cart operations.
type Option func(*Options)

// WithExpiration sets the expiration duration for a cart entry.
func WithExpiration(d time.Duration) Option {
	return func(o *Options) {
		o.Expiration = d
	}
}

// WithTags sets tags for group invalidation.
func WithTags(tags ...string) Option {
	return func(o *Options) {
		o.Tags = append(o.Tags, tags...)
	}
}

// ApplyOptions applies functional options and returns the resulting Options.
func ApplyOptions(opts ...Option) *Options {
	options := &Options{}

	for _, opt := range opts {
		opt(options)
	}

	return options
}
