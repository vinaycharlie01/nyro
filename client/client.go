package cache

import (
	"context"
)

// CacheClient is the primary interface for cache operations with entity configuration.
// It embeds the base Cache interface and adds configuration management methods,
// similar to how mongo.Client or sql.DB encapsulates all functionality.
//
// This design follows Go idioms by:
// - Providing a single entry point for all cache-related operations
// - Keeping EntityConfigManager as an internal implementation detail
// - Maintaining backward compatibility with the base Cache interface
// - Enabling easy mocking and testing
//
// Example usage:
//
//	client, err := cache.NewCacheClient(cache.CacheRedis, &cache.RedisConfig{...}, configManager)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Use as a Cache
//	err = client.Set(ctx, "user:123", user, cache.WithTTL(1*time.Hour))
//
//	// Use as a config manager
//	config, err := client.GetEntityConfig(ctx, "user")
//	client.RegisterEntity("order", cache.EntityCacheConfig{...})
type Client interface {
	// Embed the base Cache interface for backward compatibility
	// Provides: Get, Set, Delete, Clear, Exists, GetOrSet, GetMulti, SetMulti, DeleteMulti, HealthCheck, GetStats, Close
	Cache

	// Entity configuration management
	GetEntityConfig(entityName string) (EntityCacheConfig, error)
	MustGetEntityConfig(entityName string) EntityCacheConfig
	GetAllEntityConfigs() map[string]EntityCacheConfig
	SetEntityConfig(entityName string, config EntityCacheConfig)
	RegisterEntity(entityName string, config EntityCacheConfig)

	// Optional: Get the raw config manager if needed
	ConfigManager() *EntityConfigManager
}

// DefaultCacheClient is the concrete implementation of CacheClient.
// It wraps the base Cache implementation and EntityConfigManager.
type DefaultCacheClient struct {
	cache     Cache
	configMgr *EntityConfigManager
}

// NewCacheClient creates a new CacheClient with the specified backend type and configuration.
// This is the recommended factory function for creating cache clients.
func NewCacheClient(cacheType CacheType, cfg Config, configMgr *EntityConfigManager) (Client, error) {
	if configMgr == nil {
		configMgr = NewEntityConfigManager()
	}

	cache, err := New(cacheType, cfg)
	if err != nil {
		return nil, err
	}

	return &DefaultCacheClient{
		cache:     cache,
		configMgr: configMgr,
	}, nil
}

// NewCacheClientFromInstance creates a CacheClient from an already-initialized Cache instance.
// Useful when you have custom cache initialization logic.
func NewCacheClientFromInstance(cache Cache, configMgr *EntityConfigManager) Client {
	if configMgr == nil {
		configMgr = NewEntityConfigManager()
	}
	return &DefaultCacheClient{
		cache:     cache,
		configMgr: configMgr,
	}
}

// Cache interface methods (delegated to underlying cache implementation)

func (c *DefaultCacheClient) Get(ctx context.Context, key any) (any, error) {
	return c.cache.Get(ctx, key)
}

func (c *DefaultCacheClient) Set(ctx context.Context, key any, value any, opts ...Option) error {
	return c.cache.Set(ctx, key, value, opts...)
}

func (c *DefaultCacheClient) Delete(ctx context.Context, key any) error {
	return c.cache.Delete(ctx, key)
}

func (c *DefaultCacheClient) Clear(ctx context.Context) error {
	return c.cache.Clear(ctx)
}

func (c *DefaultCacheClient) Exists(ctx context.Context, key any) (bool, error) {
	return c.cache.Exists(ctx, key)
}

func (c *DefaultCacheClient) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...Option) (any, error) {
	return c.cache.GetOrSet(ctx, key, loader, opts...)
}

func (c *DefaultCacheClient) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	return c.cache.GetMulti(ctx, keys)
}

func (c *DefaultCacheClient) SetMulti(ctx context.Context, items map[any]any, opts ...Option) error {
	return c.cache.SetMulti(ctx, items, opts...)
}

func (c *DefaultCacheClient) DeleteMulti(ctx context.Context, keys []any) error {
	return c.cache.DeleteMulti(ctx, keys)
}

func (c *DefaultCacheClient) HealthCheck(ctx context.Context) error {
	return c.cache.HealthCheck(ctx)
}

func (c *DefaultCacheClient) GetStats(ctx context.Context) (*Stats, error) {
	return c.cache.GetStats(ctx)
}

func (c *DefaultCacheClient) Close() error {
	return c.cache.Close()
}

// EntityConfigManager methods (delegated to underlying config manager)

func (c *DefaultCacheClient) GetEntityConfig(entityName string) (EntityCacheConfig, error) {
	return c.configMgr.GetConfig(entityName)
}

func (c *DefaultCacheClient) MustGetEntityConfig(entityName string) EntityCacheConfig {
	return c.configMgr.MustGetConfig(entityName)
}

func (c *DefaultCacheClient) GetAllEntityConfigs() map[string]EntityCacheConfig {
	return c.configMgr.GetAllConfigs()
}

func (c *DefaultCacheClient) SetEntityConfig(entityName string, config EntityCacheConfig) {
	c.configMgr.SetConfig(entityName, config)
}

func (c *DefaultCacheClient) RegisterEntity(entityName string, config EntityCacheConfig) {
	c.configMgr.RegisterEntity(entityName, config)
}

func (c *DefaultCacheClient) ConfigManager() *EntityConfigManager {
	return c.configMgr
}
