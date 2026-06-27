package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/redis/go-redis/v9"
	valkeygo "github.com/valkey-io/valkey-go"
	cache "github.com/vinaycharlie01/nyro"
)

// CacheType identifies the cache backend.
type CacheType string

const (
	CacheRedis  CacheType = "redis"
	CacheValkey CacheType = "valkey"
	CacheMemory CacheType = "memory"
)

// Config is the interface that all cache backend configurations must implement.
type Config interface {
	LoadFromEnv() error
}

// CacheFactory creates a cache.Cache from a Config.
type CacheFactory func(cfg Config) (cache.Cache, error)

var registry sync.Map

// Register registers a CacheFactory for the given CacheType.
// Adapters call Register in their init() functions for auto-registration.
func Register(cacheType CacheType, factory CacheFactory) {
	registry.Store(cacheType, factory)
}

// New creates a cache.Cache of the specified type.
// The config is merged with values loaded from environment variables.
// An adapter package (e.g. adapters/redis) must be imported for its init() to
// call Register before New is invoked.
func New(cacheType CacheType, cfg Config) (cache.Cache, error) {
	if err := cfg.LoadFromEnv(); err != nil {
		return nil, fmt.Errorf("failed to load config from env: %w", err)
	}

	value, ok := registry.Load(cacheType)
	if !ok {
		return nil, fmt.Errorf("unsupported cache type %q — did you import the adapter package?", cacheType)
	}

	factory, ok := value.(CacheFactory)
	if !ok {
		return nil, fmt.Errorf("internal: invalid factory type registered for %q", cacheType)
	}

	return factory(cfg)
}

// RedisConfig holds Redis connection and behaviour parameters.
type RedisConfig struct {
	Addr         string        `yaml:"addr"           env:"REDIS_ADDR"`
	Password     string        `yaml:"password"       env:"REDIS_PASSWORD"`
	DB           int           `yaml:"db"             env:"REDIS_DB"`
	PoolSize     int           `yaml:"pool_size"      env:"REDIS_POOL_SIZE"`
	MinIdleConns int           `yaml:"min_idle_conns" env:"REDIS_MIN_IDLE_CONNS"`
	MaxRetries   int           `yaml:"max_retries"    env:"REDIS_MAX_RETRIES"`
	DialTimeout  time.Duration `yaml:"dial_timeout"   env:"REDIS_DIAL_TIMEOUT"`
	ReadTimeout  time.Duration `yaml:"read_timeout"   env:"REDIS_READ_TIMEOUT"`
	WriteTimeout time.Duration `yaml:"write_timeout"  env:"REDIS_WRITE_TIMEOUT"`
	DefaultTTL   time.Duration `yaml:"default_ttl"    env:"REDIS_DEFAULT_TTL"`
	// LockTTL is the lifetime of a distributed lock used for stampede prevention.
	LockTTL     time.Duration `yaml:"lock_ttl"      env:"REDIS_LOCK_TTL"`
	LockMaxWait time.Duration `yaml:"lock_max_wait" env:"REDIS_LOCK_MAX_WAIT"`
}

// LoadFromEnv implements Config.
func (c *RedisConfig) LoadFromEnv() error {
	if err := env.Parse(c); err != nil {
		return fmt.Errorf("failed to parse Redis config from env: %w", err)
	}

	return nil
}

// Client returns a *redis.Client configured from this struct.
func (c *RedisConfig) Client() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         c.Addr,
		Password:     c.Password,
		DB:           c.DB,
		PoolSize:     c.PoolSize,
		MinIdleConns: c.MinIdleConns,
		MaxRetries:   c.MaxRetries,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
	})
}

// ValkeyConfig holds Valkey connection and behaviour parameters.
// Valkey is a high-performance Redis-compatible open-source store.
type ValkeyConfig struct {
	Addr         string        `yaml:"addr"           env:"VALKEY_ADDR"`
	Password     string        `yaml:"password"       env:"VALKEY_PASSWORD"`
	DB           int           `yaml:"db"             env:"VALKEY_DB"`
	PoolSize     int           `yaml:"pool_size"      env:"VALKEY_POOL_SIZE"`
	MinIdleConns int           `yaml:"min_idle_conns" env:"VALKEY_MIN_IDLE_CONNS"`
	MaxRetries   int           `yaml:"max_retries"    env:"VALKEY_MAX_RETRIES"`
	DialTimeout  time.Duration `yaml:"dial_timeout"   env:"VALKEY_DIAL_TIMEOUT"`
	ReadTimeout  time.Duration `yaml:"read_timeout"   env:"VALKEY_READ_TIMEOUT"`
	WriteTimeout time.Duration `yaml:"write_timeout"  env:"VALKEY_WRITE_TIMEOUT"`
	DefaultTTL   time.Duration `yaml:"default_ttl"    env:"VALKEY_DEFAULT_TTL"`
	LockTTL      time.Duration `yaml:"lock_ttl"       env:"VALKEY_LOCK_TTL"`
	LockMaxWait  time.Duration `yaml:"lock_max_wait"  env:"VALKEY_LOCK_MAX_WAIT"`
}

// LoadFromEnv implements Config.
func (c *ValkeyConfig) LoadFromEnv() error {
	if err := env.Parse(c); err != nil {
		return fmt.Errorf("failed to parse Valkey config from env: %w", err)
	}

	return nil
}

// Client returns a new valkeygo.Client configured from this struct.
func (c *ValkeyConfig) Client() (valkeygo.Client, error) {
	return valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{c.Addr},
		Password:    c.Password,
		SelectDB:    c.DB,
	})
}

// MemoryConfig configures the in-memory adapter.
type MemoryConfig struct {
	// DefaultTTL is applied when Set/SetMulti are called without an explicit TTL.
	// Zero means entries never expire.
	DefaultTTL time.Duration
	// GCInterval controls how often expired entries are evicted. Defaults to 1 minute.
	GCInterval time.Duration
}

// LoadFromEnv implements Config (no-op for in-memory adapter).
func (c *MemoryConfig) LoadFromEnv() error { return nil }
