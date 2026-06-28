package surrealdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	cart "github.com/vinaycharlie01/nyro/carts"
	"golang.org/x/sync/singleflight"
)

const (
	// SurrealDBType is the identifier for the SurrealDB cart backend.
	SurrealDBType = "surrealdb"

	defaultLockTTL            = 10 * time.Second
	defaultLockMaxWait        = 3 * time.Second
	defaultLockInitialBackoff = 50 * time.Millisecond
	defaultLockMaxBackoff     = 500 * time.Millisecond
	lockKeySuffix             = ":lock"
	lockValueBytes            = 16
	defaultLockMultiplier     = 2.0
	lockRenewalDivisor        = 3
	defaultReleaseLockTimeout = 5 * time.Second

	defaultNamespace = "cache"
	defaultDatabase  = "nyro"
	defaultTable     = "cache_entries"
	lockTable        = "cache_locks"
)

// SurrealDBCartConfig holds configuration for the SurrealDB cart backend.
type SurrealDBCartConfig struct {
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
	// Namespace is the SurrealDB namespace.
	Namespace string
	// Database is the SurrealDB database name.
	Database string
	// Table is the table name for cache entries.
	Table string
}

// DefaultSurrealDBCartConfig returns the default SurrealDB cart configuration.
func DefaultSurrealDBCartConfig() *SurrealDBCartConfig {
	return &SurrealDBCartConfig{
		LockTTL:            defaultLockTTL,
		LockMaxWait:        defaultLockMaxWait,
		LockInitialBackoff: defaultLockInitialBackoff,
		LockMaxBackoff:     defaultLockMaxBackoff,
		LockMultiplier:     defaultLockMultiplier,
		Namespace:          defaultNamespace,
		Database:           defaultDatabase,
		Table:              defaultTable,
	}
}

// CacheEntry represents a cache entry in SurrealDB.
type CacheEntry struct {
	ID         string    `json:"id,omitempty"`
	Key        string    `json:"key"`
	Value      any       `json:"value"`
	Expiration time.Time `json:"expiration"`
}

// LockEntry represents a lock entry in SurrealDB.
type LockEntry struct {
	ID         string    `json:"id,omitempty"`
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	Expiration time.Time `json:"expiration"`
}

// SurrealDBClientInterface abstracts SurrealDB client operations for testing.
type SurrealDBClientInterface interface {
	Use(namespace, database string) error
	Create(table string, data any) (any, error)
	Select(table string) (any, error)
	Update(id string, data any) (any, error)
	Delete(id string) (any, error)
	Query(sql string, vars map[string]any) (any, error)
	Close()
}

// SurrealDBCart implements cart.Cart and cart.DistributedLocker for SurrealDB.
type SurrealDBCart struct {
	client *surrealdb.DB
	config *SurrealDBCartConfig
	group  singleflight.Group
}

// SurrealDBCartOption is a functional option for configuring SurrealDBCart.
type SurrealDBCartOption func(*SurrealDBCart)

// NewSurrealDB creates a new SurrealDB cart backend.
func NewSurrealDB(client *surrealdb.DB, opts ...SurrealDBCartOption) (*SurrealDBCart, error) {
	sc := &SurrealDBCart{
		client: client,
		config: DefaultSurrealDBCartConfig(),
	}

	for _, opt := range opts {
		opt(sc)
	}

	// Use namespace and database
	if err := sc.client.Use(context.Background(), sc.config.Namespace, sc.config.Database); err != nil {
		return nil, fmt.Errorf("failed to use namespace/database: %w", err)
	}

	return sc, nil
}

// WithLockTTL sets the lock TTL.
func WithLockTTL(ttl time.Duration) SurrealDBCartOption {
	return func(sc *SurrealDBCart) {
		sc.config.LockTTL = ttl
	}
}

// WithLockMaxWait sets the maximum wait time for locks.
func WithLockMaxWait(d time.Duration) SurrealDBCartOption {
	return func(sc *SurrealDBCart) {
		sc.config.LockMaxWait = d
	}
}

// WithNamespace sets the SurrealDB namespace.
func WithNamespace(ns string) SurrealDBCartOption {
	return func(sc *SurrealDBCart) {
		sc.config.Namespace = ns
	}
}

// WithDatabase sets the SurrealDB database name.
func WithDatabase(db string) SurrealDBCartOption {
	return func(sc *SurrealDBCart) {
		sc.config.Database = db
	}
}

// WithTable sets the table name for cache entries.
func WithTable(table string) SurrealDBCartOption {
	return func(sc *SurrealDBCart) {
		sc.config.Table = table
	}
}

func (sc *SurrealDBCart) Get(ctx context.Context, key string) (any, error) {
	recordID := models.NewRecordID(sc.config.Table, key)

	entry, err := surrealdb.Select[CacheEntry](ctx, sc.client, recordID)
	if err != nil {
		return nil, cart.NotFoundWithCause(err)
	}

	// Check expiration
	if time.Now().After(entry.Expiration) {
		// Delete expired entry
		_, _ = surrealdb.Delete[CacheEntry](ctx, sc.client, recordID)

		return nil, cart.NotFoundWithCause(errors.New("key expired"))
	}

	return entry.Value, nil
}

func (sc *SurrealDBCart) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	entry := CacheEntry{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(expiration),
	}

	recordID := models.NewRecordID(sc.config.Table, key)

	if _, err := surrealdb.Upsert[CacheEntry](ctx, sc.client, recordID, entry); err != nil {
		return fmt.Errorf("surrealdb: upsert failed: %w", err)
	}

	return nil
}

func (sc *SurrealDBCart) Delete(ctx context.Context, key string) error {
	recordID := models.NewRecordID(sc.config.Table, key)

	if _, err := surrealdb.Delete[CacheEntry](ctx, sc.client, recordID); err != nil {
		return fmt.Errorf("surrealdb: delete failed: %w", err)
	}

	return nil
}

func (sc *SurrealDBCart) Exists(ctx context.Context, key string) (bool, error) {
	_, err := sc.Get(ctx, key)
	if err != nil {
		var notFound *cart.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (sc *SurrealDBCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	result := make(map[string]any)

	for _, key := range keys {
		val, err := sc.Get(ctx, key)
		if err == nil {
			result[key] = val
		}
	}

	return result, nil
}

func (sc *SurrealDBCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	for key, value := range items {
		if err := sc.Set(ctx, key, value, expiration); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SurrealDBCart) DeleteMulti(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := sc.Delete(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SurrealDBCart) Clear(ctx context.Context) error {
	// Delete all records in the table
	if _, err := surrealdb.Delete[[]CacheEntry](ctx, sc.client, sc.config.Table); err != nil {
		return fmt.Errorf("surrealdb: clear failed: %w", err)
	}

	return nil
}

func (sc *SurrealDBCart) GetType() string {
	return SurrealDBType
}

func (sc *SurrealDBCart) HealthCheck(ctx context.Context) error {
	// Try a simple query to check connection
	_, err := surrealdb.Query[any](ctx, sc.client, "SELECT * FROM $table LIMIT 1", map[string]any{
		"table": sc.config.Table,
	})
	if err != nil {
		return fmt.Errorf("surrealdb: health check failed: %w", err)
	}

	return nil
}

func (sc *SurrealDBCart) Close() error {
	return sc.client.Close(context.Background())
}

// AcquireLock attempts to acquire a distributed lock.
func (sc *SurrealDBCart) AcquireLock(ctx context.Context, key string, ttl time.Duration) (lockValue string, acquired bool, err error) {
	lockKey := getLockKey(key)
	lockValue = generateLockValue()

	entry := LockEntry{
		Key:        lockKey,
		Value:      lockValue,
		Expiration: time.Now().Add(ttl),
	}

	recordID := models.NewRecordID(lockTable, lockKey)

	// Try to create lock (will fail if exists)
	_, err = surrealdb.Create[LockEntry](ctx, sc.client, recordID, entry)
	if err != nil {
		// Lock already exists
		return "", false, nil
	}

	return lockValue, true, nil
}

// ReleaseLock releases a distributed lock safely via ownership check.
func (sc *SurrealDBCart) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	lockKey := getLockKey(key)
	recordID := models.NewRecordID(lockTable, lockKey)

	// Get lock to verify ownership
	entry, err := surrealdb.Select[LockEntry](ctx, sc.client, recordID)
	if err != nil {
		return nil // Lock doesn't exist
	}

	// Check ownership
	if entry.Value != lockValue {
		return nil // Not our lock
	}

	// Delete lock
	if _, err := surrealdb.Delete[LockEntry](ctx, sc.client, recordID); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// ExtendLock extends the TTL of an existing lock via ownership check.
func (sc *SurrealDBCart) ExtendLock(ctx context.Context, key string, lockValue string, ttl time.Duration) (bool, error) {
	lockKey := getLockKey(key)
	recordID := models.NewRecordID(lockTable, lockKey)

	// Get lock to verify ownership
	entry, err := surrealdb.Select[LockEntry](ctx, sc.client, recordID)
	if err != nil {
		return false, nil // Lock doesn't exist
	}

	// Check ownership
	if entry.Value != lockValue {
		return false, nil // Not our lock
	}

	// Update expiration
	entry.Expiration = time.Now().Add(ttl)
	if _, err := surrealdb.Update[LockEntry](ctx, sc.client, recordID, entry); err != nil {
		return false, fmt.Errorf("failed to extend lock: %w", err)
	}

	return true, nil
}

// GetOrSetWithLock retrieves a cached value or populates it with distributed lock protection.
func (sc *SurrealDBCart) GetOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	value, err := sc.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	var notFoundErr *cart.NotFound
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("cache get error: %w", err)
	}

	resultCh := sc.group.DoChan(key, func() (any, error) {
		return sc.getOrSetWithLock(ctx, key, loader, expiration, lockTTL)
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
func (sc *SurrealDBCart) getOrSetWithLock(
	ctx context.Context,
	key string,
	loader func(context.Context) (any, error),
	expiration time.Duration,
	lockTTL time.Duration,
) (any, error) {
	if lockTTL == 0 {
		lockTTL = sc.config.LockTTL
	}

	lockValue, acquired, err := sc.AcquireLock(ctx, key, lockTTL)
	if err != nil {
		return nil, fmt.Errorf("lock acquisition failed: %w", err)
	}

	if !acquired {
		return sc.waitForCache(ctx, key)
	}

	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), defaultReleaseLockTimeout)
		defer cancel()

		_ = sc.ReleaseLock(releaseCtx, key, lockValue)
	}()

	if cachedValue, getErr := sc.Get(ctx, key); getErr == nil {
		return cachedValue, nil
	}

	renewalInterval := sc.config.LockRenewalInterval
	if renewalInterval <= 0 {
		renewalInterval = lockTTL / lockRenewalDivisor
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)

	go sc.startLockHeartbeat(heartbeatCtx, key, lockValue, lockTTL, renewalInterval)

	result, loaderErr := loader(ctx)
	cancelHeartbeat()

	if loaderErr != nil {
		return nil, fmt.Errorf("loader failed: %w", loaderErr)
	}

	if result == nil {
		return nil, errors.New("loader returned nil result")
	}

	if setErr := sc.Set(ctx, key, result, expiration); setErr != nil {
		_ = setErr
	}

	return result, nil
}

func (sc *SurrealDBCart) waitForCache(ctx context.Context, key string) (any, error) {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = sc.config.LockInitialBackoff
	expBackoff.MaxInterval = sc.config.LockMaxBackoff
	expBackoff.MaxElapsedTime = sc.config.LockMaxWait
	expBackoff.Multiplier = sc.config.LockMultiplier
	expBackoff.RandomizationFactor = 0.5

	ctxBackoff := backoff.WithContext(expBackoff, ctx)

	var value any

	operation := func() error {
		v, err := sc.Get(ctx, key)
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
		return nil, mapWaitError(err, sc.config.LockMaxWait)
	}

	return value, nil
}

func (sc *SurrealDBCart) startLockHeartbeat(
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
			extended, err := sc.ExtendLock(ctx, key, lockValue, lockTTL)
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
