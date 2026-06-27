package cache_adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	cache "github.com/vinaycharlie01/nyro"
	"github.com/vinaycharlie01/nyro/config"
	"github.com/vinaycharlie01/nyro/stores/valkey"
)

func init() {
	// Register Valkey cache factory - enables pluggable cache backend swapping
	// Add new backends: 1) Implement Store interface 2) Create adapter 3) Register
	Register(config.CacheValkey, func(cfg config.Config) (cache.Cache, error) {
		valkeyConfig, ok := cfg.(*config.ValkeyConfig)
		if !ok {
			return nil, fmt.Errorf("invalid config type for Valkey cache")
		}
		return NewValkeyAdapter(*valkeyConfig)
	})
}

// ValkeyAdapter implements Cache interface using Valkey store.
// Adapter pattern: wraps ValkeyStore, handles key conversion and TTL management.
type ValkeyAdapter struct {
	store  *valkey.ValkeyStore
	config config.ValkeyConfig
}

// NewValkeyAdapter creates a new Valkey adapter
func NewValkeyAdapter(config config.ValkeyConfig) (*ValkeyAdapter, error) {
	// Create Valkey client
	client, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{config.Addr},
		Password:    config.Password,
		SelectDB:    config.DB,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pingCmd := client.B().Ping().Build()
	if err := client.Do(ctx, pingCmd).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	// Create Valkey store with configuration
	var storeOpts []valkey.ValkeyStoreOption
	if config.LockTTL > 0 {
		storeOpts = append(storeOpts, valkey.WithLockTTL(config.LockTTL))
	}
	if config.LockMaxWait > 0 {
		storeOpts = append(storeOpts, valkey.WithLockMaxWait(config.LockMaxWait))
	}

	store := valkey.NewValkey(client, storeOpts...)

	return &ValkeyAdapter{
		store:  store,
		config: config,
	}, nil
}

// Get retrieves a value from cache
func (a *ValkeyAdapter) Get(ctx context.Context, key any) (any, error) {
	strKey := keyToString(key)
	return a.store.Get(ctx, strKey)
}

// Set stores a value in cache
func (a *ValkeyAdapter) Set(ctx context.Context, key any, value any, options ...Option) error {
	strKey := keyToString(key)

	// Apply options to get expiration
	opts := ApplyOptions(options...)
	expiration := opts.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour // Default to 24 hours if not configured
	}

	return a.store.Set(ctx, strKey, value, expiration)
}

// Delete removes a key from cache
func (a *ValkeyAdapter) Delete(ctx context.Context, key any) error {
	strKey := keyToString(key)
	return a.store.Delete(ctx, strKey)
}

// Clear removes all keys from cache
func (a *ValkeyAdapter) Clear(ctx context.Context) error {
	return a.store.Clear(ctx)
}

// GetMulti retrieves multiple values from cache
func (a *ValkeyAdapter) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	if len(keys) == 0 {
		return make(map[any]any), nil
	}

	// Convert any keys to string keys
	strKeys := make([]string, len(keys))
	keyMap := make(map[string]any) // Map string key back to original key
	for i, key := range keys {
		strKey := keyToString(key)
		strKeys[i] = strKey
		keyMap[strKey] = key
	}

	// Get from store
	storeResult, err := a.store.GetMulti(ctx, strKeys)
	if err != nil {
		return nil, err
	}

	// Convert back to original key type
	result := make(map[any]any, len(storeResult))
	for strKey, value := range storeResult {
		originalKey := keyMap[strKey]
		result[originalKey] = value
	}

	return result, nil
}

// SetMulti stores multiple values in cache
func (a *ValkeyAdapter) SetMulti(ctx context.Context, items map[any]any, options ...Option) error {
	if len(items) == 0 {
		return nil
	}

	// Apply options to get expiration
	opts := ApplyOptions(options...)
	expiration := opts.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour
	}

	// Convert any keys to string keys
	storeItems := make(map[string]any, len(items))
	for key, value := range items {
		strKey := keyToString(key)
		storeItems[strKey] = value
	}

	return a.store.SetMulti(ctx, storeItems, expiration)
}

// DeleteMulti removes multiple keys from cache
func (a *ValkeyAdapter) DeleteMulti(ctx context.Context, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	// Convert any keys to string keys
	strKeys := make([]string, len(keys))
	for i, key := range keys {
		strKeys[i] = keyToString(key)
	}

	return a.store.DeleteMulti(ctx, strKeys)
}

// Exists checks if a key exists in cache
func (a *ValkeyAdapter) Exists(ctx context.Context, key any) (bool, error) {
	strKey := keyToString(key)
	return a.store.Exists(ctx, strKey)
}

// GetOrSet retrieves a value from cache, or sets it if not found
// Uses distributed locking to prevent cache stampede
func (a *ValkeyAdapter) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...Option) (any, error) {
	strKey := keyToString(key)

	// Apply options to get expiration
	options := ApplyOptions(opts...)
	expiration := options.Expiration
	if expiration == 0 {
		expiration = a.config.DefaultTTL
	}
	if expiration == 0 {
		expiration = 24 * time.Hour
	}

	// Get lock TTL from config or use default
	lockTTL := a.config.LockTTL
	if lockTTL == 0 {
		lockTTL = 10 * time.Second // Default lock TTL
	}

	// Use distributed locking for cache stampede prevention
	return a.store.GetOrSetWithLock(ctx, strKey, loader, expiration, lockTTL)
}

// Type returns the cache backend type
func (a *ValkeyAdapter) Type() CacheType {
	return CacheValkey
}

// GetStats returns cache statistics
func (a *ValkeyAdapter) GetStats(ctx context.Context) (*Stats, error) {
	// Check health to determine connection status
	if err := a.store.HealthCheck(ctx); err != nil {
		return &Stats{
			Type:      string(CacheValkey),
			Connected: false,
		}, nil
	}

	return &Stats{
		Type:      string(CacheValkey),
		Connected: true,
	}, nil
}

// Close closes the Valkey connection
func (a *ValkeyAdapter) Close() error {
	if a.store == nil {
		return nil
	}
	return a.store.Close()
}

// HealthCheck checks if Valkey is accessible
func (a *ValkeyAdapter) HealthCheck(ctx context.Context) error {
	if a.store == nil {
		return errors.New("store is nil")
	}
	return a.store.HealthCheck(ctx)
}
