package memcached

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cenkalti/backoff/v4"
	cart "github.com/vinaycharlie01/nyro/carts"
	"golang.org/x/sync/singleflight"
)

const (
	// MemcachedType is the identifier for the Memcached cart backend.
	MemcachedType = "memcached"

	defaultLockTTL            = 10 * time.Second
	defaultLockMaxWait        = 3 * time.Second
	defaultLockInitialBackoff = 50 * time.Millisecond
	defaultLockMaxBackoff     = 500 * time.Millisecond
	lockKeySuffix             = ":lock"
	lockValueBytes            = 16
	defaultLockMultiplier     = 2.0
	lockRenewalDivisor        = 3
)

// MemcachedCartConfig holds configuration for the Memcached cart backend.
type MemcachedCartConfig struct {
	// LockTTL is the TTL for distributed locks (default: 10s).
	LockTTL time.Duration
	// LockMaxWait is the maximum time to wait for a lock holder to finish (default: 3s).
	LockMaxWait time.Duration
	// LockInitialBackoff is the initial backoff interval (default: 50ms).
	LockInitialBackoff time.Duration
	// LockMaxBackoff is the maximum backoff interval (default: 500ms).
	LockMaxBackoff time.Duration
	// LockMultiplier is the exponential backoff multiplier (default: 2.0).
	LockMultiplier float64
	// LockRenewalInterval is the heartbeat renewal interval (default: LockTTL/3).
	LockRenewalInterval time.Duration
}

// DefaultMemcachedCartConfig returns the default Memcached cart configuration.
func DefaultMemcachedCartConfig() *MemcachedCartConfig {
	return &MemcachedCartConfig{
		LockTTL:            defaultLockTTL,
		LockMaxWait:        defaultLockMaxWait,
		LockInitialBackoff: defaultLockInitialBackoff,
		LockMaxBackoff:     defaultLockMaxBackoff,
		LockMultiplier:     defaultLockMultiplier,
	}
}

// MemcachedClientInterface abstracts Memcached client operations for testing.
type MemcachedClientInterface interface {
	Get(key string) (*memcache.Item, error)
	Set(item *memcache.Item) error
	Add(item *memcache.Item) error
	Delete(key string) error
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	FlushAll() error
	Ping() error
}

// Ensure *memcache.Client implements MemcachedClientInterface
var _ MemcachedClientInterface = (*memcache.Client)(nil)

// MemcachedCart implements cart.Cart and cart.DistributedLocker for Memcached.
type MemcachedCart struct {
	client MemcachedClientInterface
	config *MemcachedCartConfig
	group  singleflight.Group
}

// MemcachedCartOption is a functional option for configuring MemcachedCart.
type MemcachedCartOption func(*MemcachedCart)

// NewMemcached creates a new Memcached cart backend.
func NewMemcached(client MemcachedClientInterface, opts ...MemcachedCartOption) *MemcachedCart {
	mc := &MemcachedCart{
		client: client,
		config: DefaultMemcachedCartConfig(),
	}

	for _, opt := range opts {
		opt(mc)
	}

	return mc
}

// WithLockTTL sets the lock TTL.
func WithLockTTL(ttl time.Duration) MemcachedCartOption {
	return func(mc *MemcachedCart) {
		mc.config.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum wait time for locks.
func WithLockMaxWait(d time.Duration) MemcachedCartOption {
	return func(mc *MemcachedCart) {
		mc.config.LockMaxWait = d
	}
}

func (mc *MemcachedCart) Get(ctx context.Context, key string) (any, error) {
	item, err := mc.client.Get(key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, cart.NotFoundWithCause(err)
		}

		return nil, fmt.Errorf("memcached: get failed: %w", err)
	}

	var value any
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return nil, fmt.Errorf("memcached: unmarshal failed: %w", err)
	}

	return value, nil
}

func (mc *MemcachedCart) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("memcached: marshal failed: %w", err)
	}

	item := &memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: int32(expiration.Seconds()),
	}

	if err := mc.client.Set(item); err != nil {
		return fmt.Errorf("memcached: set failed: %w", err)
	}

	return nil
}

func (mc *MemcachedCart) Delete(ctx context.Context, key string) error {
	if err := mc.client.Delete(key); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return fmt.Errorf("memcached: delete failed: %w", err)
	}

	return nil
}

func (mc *MemcachedCart) Exists(ctx context.Context, key string) (bool, error) {
	_, err := mc.client.Get(key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return false, nil
		}

		return false, fmt.Errorf("memcached: exists check failed: %w", err)
	}

	return true, nil
}

func (mc *MemcachedCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	items, err := mc.client.GetMulti(keys)
	if err != nil {
		return nil, fmt.Errorf("memcached: get multi failed: %w", err)
	}

	result := make(map[string]any, len(items))
	for key, item := range items {
		var value any
		if err := json.Unmarshal(item.Value, &value); err != nil {
			return nil, fmt.Errorf("memcached: unmarshal failed for key %s: %w", key, err)
		}

		result[key] = value
	}

	return result, nil
}

func (mc *MemcachedCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	for key, value := range items {
		if err := mc.Set(ctx, key, value, expiration); err != nil {
			return err
		}
	}

	return nil
}

func (mc *MemcachedCart) DeleteMulti(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := mc.Delete(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

func (mc *MemcachedCart) Clear(ctx context.Context) error {
	if err := mc.client.FlushAll(); err != nil {
		return fmt.Errorf("memcached: flush all failed: %w", err)
	}

	return nil
}

func (mc *MemcachedCart) GetType() string {
	return MemcachedType
}

func (mc *MemcachedCart) HealthCheck(ctx context.Context) error {
	if err := mc.client.Ping(); err != nil {
		return fmt.Errorf("memcached: health check failed: %w", err)
	}

	return nil
}

// AcquireLock attempts to acquire a distributed lock using Memcached ADD command.
func (mc *MemcachedCart) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockKey := getLockKey(key)
	lockValue = generateLockValue()

	item := &memcache.Item{
		Key:        lockKey,
		Value:      []byte(lockValue),
		Expiration: int32(ttl.Seconds()),
	}

	err = mc.client.Add(item)
	if err != nil {
		if errors.Is(err, memcache.ErrNotStored) {
			return "", false, nil
		}

		return "", false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return lockValue, true, nil
}

// ReleaseLock releases a distributed lock safely via ownership check.
func (mc *MemcachedCart) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	lockKey := getLockKey(key)

	item, err := mc.client.Get(lockKey)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil
		}

		return fmt.Errorf("failed to get lock for release: %w", err)
	}

	if string(item.Value) != lockValue {
		return nil
	}

	if err := mc.client.Delete(lockKey); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock via ownership check.
func (mc *MemcachedCart) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	lockKey := getLockKey(key)

	item, err := mc.client.Get(lockKey)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return false, nil
		}

		return false, fmt.Errorf("failed to get lock for extension: %w", err)
	}

	if string(item.Value) != lockValue {
		return false, nil
	}

	item.Expiration = int32(ttl.Seconds())
	if err := mc.client.Set(item); err != nil {
		return false, fmt.Errorf("failed to extend lock: %w", err)
	}

	return true, nil
}

// GetOrSetWithLock retrieves a cached value or populates it with distributed lock protection.
func (mc *MemcachedCart) GetOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	value, err := mc.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFoundErr *cart.NotFound
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("cache get error: %w", err)
	}

	resultCh := mc.group.DoChan(key, func() (any, error) {
		return mc.getOrSetWithLock(ctx, key, loader, expiration, lockTTL)
	})

	select {
	case r := <-resultCh:
		if r.Err != nil {
			return nil, r.Err
		}

		return r.Val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

//nolint:contextcheck // Using Background() in defer is intentional
func (mc *MemcachedCart) getOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	if lockTTL == 0 {
		lockTTL = mc.config.LockTTL
	}

	lockValue, acquired, err := mc.AcquireLock(ctx, key, lockTTL)
	if err != nil {
		return nil, fmt.Errorf("lock acquisition failed: %w", err)
	}

	if !acquired {
		return mc.waitForCache(ctx, key)
	}

	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if releaseErr := mc.ReleaseLock(releaseCtx, key, lockValue); releaseErr != nil {
			// Log error but don't fail
			_ = releaseErr
		}
	}()

	if cachedValue, getErr := mc.Get(ctx, key); getErr == nil {
		return cachedValue, nil
	}

	renewalInterval := mc.config.LockRenewalInterval
	if renewalInterval <= 0 {
		renewalInterval = lockTTL / lockRenewalDivisor
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)

	go mc.startLockHeartbeat(heartbeatCtx, key, lockValue, lockTTL, renewalInterval)

	result, loaderErr := loader(ctx)
	cancelHeartbeat()

	if loaderErr != nil {
		return nil, fmt.Errorf("loader failed: %w", loaderErr)
	}

	if result == nil {
		return nil, errors.New("loader returned nil result")
	}

	if setErr := mc.Set(ctx, key, result, expiration); setErr != nil {
		// Log error but return result
		_ = setErr
	}

	return result, nil
}

func (mc *MemcachedCart) waitForCache(ctx context.Context, key string) (any, error) {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = mc.config.LockInitialBackoff
	expBackoff.MaxInterval = mc.config.LockMaxBackoff
	expBackoff.MaxElapsedTime = mc.config.LockMaxWait
	expBackoff.Multiplier = mc.config.LockMultiplier
	expBackoff.RandomizationFactor = 0.5

	ctxBackoff := backoff.WithContext(expBackoff, ctx)

	var value any

	operation := func() error {
		v, err := mc.Get(ctx, key)
		if err == nil {
			value = v

			return nil
		}

		var notFound *cart.NotFound
		if errors.As(err, &notFound) {
			return err
		}

		return backoff.Permanent(err)
	}

	if err := backoff.Retry(operation, ctxBackoff); err != nil {
		return nil, mapWaitError(err, mc.config.LockMaxWait)
	}

	return value, nil
}

func (mc *MemcachedCart) startLockHeartbeat(
	ctx context.Context,
	key, lockValue string,
	lockTTL, renewalInterval time.Duration,
) {
	ticker := time.NewTicker(renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			extended, err := mc.ExtendLock(ctx, key, lockValue, lockTTL)
			if err != nil || !extended {
				return
			}
		}
	}
}

func getLockKey(key string) string {
	return key + lockKeySuffix
}

func generateLockValue() string {
	b := make([]byte, lockValueBytes)

	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}

func mapWaitError(err error, maxWait time.Duration) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("timeout waiting for cache population after %v", maxWait)
	case errors.Is(err, context.Canceled):
		return err
	default:
		return fmt.Errorf("failed to wait for cache: %w", err)
	}
}

func (mc *MemcachedCart) Close() error {
	// gomemcache doesn't have a Close method
	return nil
}
