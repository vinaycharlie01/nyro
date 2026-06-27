// Package client provides a high-level cache client that bundles a cache.Cache
// implementation with per-entity configuration management.
package client

import (
	"context"

	cache "github.com/vinaycharlie01/nyro"
	"github.com/vinaycharlie01/nyro/config"
)

// Client bundles a cache.Cache backend with entity-level configuration.
// It satisfies cache.Cache so it can be used as a drop-in replacement.
type Client interface {
	cache.Cache

	// Entity configuration
	GetEntityConfig(entityName string) (config.EntityCacheConfig, error)
	MustGetEntityConfig(entityName string) config.EntityCacheConfig
	GetAllEntityConfigs() map[string]config.EntityCacheConfig
	SetEntityConfig(entityName string, cfg config.EntityCacheConfig)
	RegisterEntity(entityName string, cfg config.EntityCacheConfig)
	ConfigManager() *config.EntityConfigManager
}

type defaultClient struct {
	cache     cache.Cache
	configMgr *config.EntityConfigManager
}

// New creates a Client backed by the specified cache type and configuration.
// An adapter package must be imported (e.g. import _ "adapters/redis") for its
// init() to register the factory before New is called.
func New(cacheType config.CacheType, cfg config.Config, configMgr *config.EntityConfigManager) (Client, error) {
	if configMgr == nil {
		configMgr = config.NewEntityConfigManager()
	}

	c, err := config.New(cacheType, cfg)
	if err != nil {
		return nil, err
	}

	return &defaultClient{cache: c, configMgr: configMgr}, nil
}

// NewFromInstance creates a Client from an already-initialised cache.Cache.
func NewFromInstance(c cache.Cache, configMgr *config.EntityConfigManager) Client {
	if configMgr == nil {
		configMgr = config.NewEntityConfigManager()
	}

	return &defaultClient{cache: c, configMgr: configMgr}
}

// cache.Cache delegation

func (c *defaultClient) Get(ctx context.Context, key any) (any, error) {
	return c.cache.Get(ctx, key)
}

func (c *defaultClient) Set(ctx context.Context, key any, value any, opts ...cache.Option) error {
	return c.cache.Set(ctx, key, value, opts...)
}

func (c *defaultClient) Delete(ctx context.Context, key any) error {
	return c.cache.Delete(ctx, key)
}

func (c *defaultClient) Clear(ctx context.Context) error {
	return c.cache.Clear(ctx)
}

func (c *defaultClient) Exists(ctx context.Context, key any) (bool, error) {
	return c.cache.Exists(ctx, key)
}

func (c *defaultClient) GetOrSet(ctx context.Context, key any, loader func(context.Context) (any, error), opts ...cache.Option) (any, error) {
	return c.cache.GetOrSet(ctx, key, loader, opts...)
}

func (c *defaultClient) GetMulti(ctx context.Context, keys []any) (map[any]any, error) {
	return c.cache.GetMulti(ctx, keys)
}

func (c *defaultClient) SetMulti(ctx context.Context, items map[any]any, opts ...cache.Option) error {
	return c.cache.SetMulti(ctx, items, opts...)
}

func (c *defaultClient) DeleteMulti(ctx context.Context, keys []any) error {
	return c.cache.DeleteMulti(ctx, keys)
}

func (c *defaultClient) HealthCheck(ctx context.Context) error {
	return c.cache.HealthCheck(ctx)
}

func (c *defaultClient) GetStats(ctx context.Context) (*cache.Stats, error) {
	return c.cache.GetStats(ctx)
}

func (c *defaultClient) Close() error {
	return c.cache.Close()
}

// Entity config management

func (c *defaultClient) GetEntityConfig(entityName string) (config.EntityCacheConfig, error) {
	return c.configMgr.GetConfig(entityName)
}

func (c *defaultClient) MustGetEntityConfig(entityName string) config.EntityCacheConfig {
	return c.configMgr.MustGetConfig(entityName)
}

func (c *defaultClient) GetAllEntityConfigs() map[string]config.EntityCacheConfig {
	return c.configMgr.GetAllConfigs()
}

func (c *defaultClient) SetEntityConfig(entityName string, cfg config.EntityCacheConfig) {
	c.configMgr.SetConfig(entityName, cfg)
}

func (c *defaultClient) RegisterEntity(entityName string, cfg config.EntityCacheConfig) {
	c.configMgr.RegisterEntity(entityName, cfg)
}

func (c *defaultClient) ConfigManager() *config.EntityConfigManager {
	return c.configMgr
}
