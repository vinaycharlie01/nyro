// Package olric provides an Olric-backed cart implementation with distributed locking.
package olric

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/olric-data/olric"
	cart "github.com/vinaycharlie01/nyro/carts"
	"golang.org/x/sync/singleflight"
)

const (
	// DefaultDMapName is the default DMap name for cache entries.
	DefaultDMapName = "cache"
	// DefaultLockTTL is the default lock time-to-live.
	DefaultLockTTL = 10 * time.Second
	// DefaultLockMaxWait is the default maximum wait time for acquiring a lock.
	DefaultLockMaxWait = 30 * time.Second
)

// OlricClientInterface defines the interface for Olric client operations.
// This allows for easier testing and mocking.
type OlricClientInterface interface {
	NewDMap(name string) (olric.DMap, error)
	Close(ctx context.Context) error
}

// OlricCart implements cart.Cart and cart.DistributedLocker for Olric.
type OlricCart struct {
	client  OlricClientInterface
	dmap    olric.DMap
	config  *OlricCartConfig
	sf      *singleflight.Group
	locksMu sync.RWMutex
	locks   map[string]context.CancelFunc
}

// OlricCartConfig holds configuration for OlricCart.
type OlricCartConfig struct {
	DMapName    string
	LockTTL     time.Duration
	LockMaxWait time.Duration
}

// DefaultOlricCartConfig returns default configuration.
func DefaultOlricCartConfig() *OlricCartConfig {
	return &OlricCartConfig{
		DMapName:    DefaultDMapName,
		LockTTL:     DefaultLockTTL,
		LockMaxWait: DefaultLockMaxWait,
	}
}

// OlricCartOption is a functional option for configuring OlricCart.
type OlricCartOption func(*OlricCart)

// NewOlric creates a new Olric cart backend.
func NewOlric(client OlricClientInterface, opts ...OlricCartOption) (*OlricCart, error) {
	oc := &OlricCart{
		client: client,
		config: DefaultOlricCartConfig(),
		sf:     &singleflight.Group{},
		locks:  make(map[string]context.CancelFunc),
	}

	for _, opt := range opts {
		opt(oc)
	}

	// Create DMap
	dmap, err := client.NewDMap(oc.config.DMapName)
	if err != nil {
		return nil, fmt.Errorf("failed to create DMap: %w", err)
	}
	oc.dmap = dmap

	return oc, nil
}

// WithDMapName sets the DMap name.
func WithDMapName(name string) OlricCartOption {
	return func(oc *OlricCart) {
		oc.config.DMapName = name
	}
}

// WithLockTTL sets the lock TTL.
func WithLockTTL(ttl time.Duration) OlricCartOption {
	return func(oc *OlricCart) {
		oc.config.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum wait time for locks.
func WithLockMaxWait(d time.Duration) OlricCartOption {
	return func(oc *OlricCart) {
		oc.config.LockMaxWait = d
	}
}

// Get retrieves a value from the cache.
func (oc *OlricCart) Get(ctx context.Context, key string) (any, error) {
	resp, err := oc.dmap.Get(ctx, key)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return nil, cart.NotFoundWithCause(err)
		}
		return nil, fmt.Errorf("olric: get failed: %w", err)
	}

	var value any
	if err := resp.Scan(&value); err != nil {
		return nil, fmt.Errorf("olric: scan failed: %w", err)
	}

	return value, nil
}

// Set stores a value in the cache with expiration.
func (oc *OlricCart) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	opts := []olric.PutOption{olric.EX(expiration)}
	if err := oc.dmap.Put(ctx, key, value, opts...); err != nil {
		return fmt.Errorf("olric: put failed: %w", err)
	}

	return nil
}

// Delete removes a value from the cache.
func (oc *OlricCart) Delete(ctx context.Context, key string) error {
	if _, err := oc.dmap.Delete(ctx, key); err != nil {
		return fmt.Errorf("olric: delete failed: %w", err)
	}

	return nil
}

// Exists checks if a key exists in the cache.
func (oc *OlricCart) Exists(ctx context.Context, key string) (bool, error) {
	_, err := oc.dmap.Get(ctx, key)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("olric: exists check failed: %w", err)
	}

	return true, nil
}

// GetMulti retrieves multiple values from the cache.
func (oc *OlricCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	result := make(map[string]any, len(keys))

	for _, key := range keys {
		value, err := oc.Get(ctx, key)
		if err != nil {
			var notFound *cart.NotFound
			if errors.As(err, &notFound) {
				continue
			}
			return nil, err
		}
		result[key] = value
	}

	return result, nil
}

// SetMulti stores multiple values in the cache.
func (oc *OlricCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	for key, value := range items {
		if err := oc.Set(ctx, key, value, expiration); err != nil {
			return err
		}
	}

	return nil
}

// DeleteMulti removes multiple values from the cache.
func (oc *OlricCart) DeleteMulti(ctx context.Context, keys []string) error {
	if _, err := oc.dmap.Delete(ctx, keys...); err != nil {
		return fmt.Errorf("olric: delete multi failed: %w", err)
	}

	return nil
}

// Clear removes all entries from the cache.
func (oc *OlricCart) Clear(ctx context.Context) error {
	if err := oc.dmap.Destroy(ctx); err != nil {
		return fmt.Errorf("olric: clear failed: %w", err)
	}

	// Recreate the DMap
	dmap, err := oc.client.NewDMap(oc.config.DMapName)
	if err != nil {
		return fmt.Errorf("olric: failed to recreate DMap: %w", err)
	}
	oc.dmap = dmap

	return nil
}

// HealthCheck verifies the connection to Olric.
func (oc *OlricCart) HealthCheck(ctx context.Context) error {
	// Try a simple operation
	testKey := "__health_check__"
	if err := oc.dmap.Put(ctx, testKey, "ok", olric.EX(time.Second)); err != nil {
		return fmt.Errorf("olric: health check failed: %w", err)
	}

	_, _ = oc.dmap.Delete(ctx, testKey)

	return nil
}

// Close closes the Olric connection.
func (oc *OlricCart) Close() error {
	// Cancel all active locks
	oc.locksMu.Lock()
	for _, cancel := range oc.locks {
		cancel()
	}
	oc.locks = make(map[string]context.CancelFunc)
	oc.locksMu.Unlock()

	return oc.dmap.Close(context.Background())
}

// AcquireLock attempts to acquire a distributed lock.
func (oc *OlricCart) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockValue = generateLockValue()

	lockCtx, err := oc.dmap.LockWithTimeout(ctx, key, ttl, oc.config.LockMaxWait)
	if err != nil {
		if errors.Is(err, olric.ErrLockNotAcquired) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Store lock context for cleanup
	oc.locksMu.Lock()
	oc.locks[key] = func() {
		_ = lockCtx.Unlock(context.Background())
	}
	oc.locksMu.Unlock()

	return lockValue, true, nil
}

// ReleaseLock releases a distributed lock.
func (oc *OlricCart) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	oc.locksMu.Lock()
	cancel, exists := oc.locks[key]
	if exists {
		delete(oc.locks, key)
	}
	oc.locksMu.Unlock()

	if exists {
		cancel()
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock.
func (oc *OlricCart) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	// Olric doesn't support extending locks directly
	// We need to release and reacquire
	oc.ReleaseLock(ctx, key, lockValue)

	newLockValue, acquired, err := oc.AcquireLock(ctx, key, ttl)
	if err != nil {
		return false, err
	}

	if !acquired {
		return false, nil
	}

	// Update lock value tracking
	_ = newLockValue

	return true, nil
}

// GetOrSetWithLock retrieves a value or sets it using a loader function with distributed locking.
func (oc *OlricCart) GetOrSetWithLock(ctx context.Context, key string, loader func(context.Context) (any, error), ttl, lockTTL time.Duration) (any, error) {
	// Try to get the value first
	value, err := oc.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFound *cart.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	// Use singleflight to prevent duplicate work
	result, err, _ := oc.sf.Do(key, func() (any, error) {
		// Double-check after acquiring singleflight
		value, err := oc.Get(ctx, key)
		if err == nil {
			return value, nil
		}

		var nf *cart.NotFound
		if !errors.As(err, &nf) {
			return nil, err
		}

		// Acquire distributed lock
		lockValue, acquired, err := oc.AcquireLock(ctx, key, lockTTL)
		if err != nil {
			return nil, err
		}

		if !acquired {
			// Wait and retry
			time.Sleep(100 * time.Millisecond)
			return oc.Get(ctx, key)
		}

		defer oc.ReleaseLock(ctx, key, lockValue)

		// Triple-check after acquiring lock
		value, err = oc.Get(ctx, key)
		if err == nil {
			return value, nil
		}

		var nf2 *cart.NotFound
		if !errors.As(err, &nf2) {
			return nil, err
		}

		// Load the value
		value, err = loader(ctx)
		if err != nil {
			return nil, err
		}

		// Store the value
		if err := oc.Set(ctx, key, value, ttl); err != nil {
			return nil, err
		}

		return value, nil
	})

	return result, err
}

// generateLockValue generates a unique lock value.
func generateLockValue() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
