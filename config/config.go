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

// CacheType represents the type of cache backend
type CacheType string

const (
	CacheRedis  CacheType = "redis"
	CacheValkey CacheType = "valkey"
)

// Config is the interface that all cache configurations must implement
type Config interface {
	LoadFromEnv() error
}

// CacheFactory is a function that creates a cache instance from a config
type CacheFactory func(cfg Config) (cache.Cache, error)

var (
	registry sync.Map
)

// Register registers a cache factory for a specific cache type
// This allows pluggable cache implementations
func Register(cacheType CacheType, factory CacheFactory) {
	registry.Store(cacheType, factory)
}

// New creates a cache instance of the specified type with the given configuration
// The configuration is automatically merged with environment variables
//
// Example:
//
//	cache, err := cache.New(cache.CacheRedis, &cache.RedisConfig{
//	    Addr: "localhost:6379",
//	    DB: 0,
//	})
func New(cacheType CacheType, cfg Config) (cache.Cache, error) {
	// Load config from environment variables
	if err := cfg.LoadFromEnv(); err != nil {
		return nil, fmt.Errorf("failed to load config from env: %w", err)
	}

	value, ok := registry.Load(cacheType)
	if !ok {
		return nil, fmt.Errorf("unsupported cache type: %s", cacheType)
	}

	factory, ok := value.(CacheFactory)
	if !ok {
		return nil, fmt.Errorf("invalid factory type for cache: %s", cacheType)
	}

	return factory(cfg)
}

// RedisConfig represents Redis-specific configuration
type RedisConfig struct {
	Addr         string        `yaml:"addr" env:"REDIS_ADDR"`
	Password     string        `yaml:"password" env:"REDIS_PASSWORD"`
	DB           int           `yaml:"db" env:"REDIS_DB"`
	PoolSize     int           `yaml:"pool_size" env:"REDIS_POOL_SIZE"`
	MinIdleConns int           `yaml:"min_idle_conns" env:"REDIS_MIN_IDLE_CONNS"`
	MaxRetries   int           `yaml:"max_retries" env:"REDIS_MAX_RETRIES"`
	DialTimeout  time.Duration `yaml:"dial_timeout" env:"REDIS_DIAL_TIMEOUT"`
	ReadTimeout  time.Duration `yaml:"read_timeout" env:"REDIS_READ_TIMEOUT"`
	WriteTimeout time.Duration `yaml:"write_timeout" env:"REDIS_WRITE_TIMEOUT"`
	DefaultTTL   time.Duration `yaml:"default_ttl" env:"REDIS_DEFAULT_TTL"`

	// Distributed locking configuration (cache stampede prevention)
	LockTTL     time.Duration `yaml:"lock_ttl" env:"REDIS_LOCK_TTL"`           // TTL for distributed locks (default: 10s)
	LockMaxWait time.Duration `yaml:"lock_max_wait" env:"REDIS_LOCK_MAX_WAIT"` // Max time to wait for lock holder (default: 3s)
}

// LoadFromEnv implements Config interface for RedisConfig
func (c *RedisConfig) LoadFromEnv() error {
	if err := env.Parse(c); err != nil {
		return fmt.Errorf("failed to parse Redis config from env: %w", err)
	}
	return nil
}

// Client creates and returns a Redis client from the configuration
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

// ValkeyConfig represents Valkey-specific configuration
// Valkey is a high-performance fork of Redis, fully compatible with Redis protocol
type ValkeyConfig struct {
	Addr         string        `yaml:"addr" env:"VALKEY_ADDR"`
	Password     string        `yaml:"password" env:"VALKEY_PASSWORD"`
	DB           int           `yaml:"db" env:"VALKEY_DB"`
	PoolSize     int           `yaml:"pool_size" env:"VALKEY_POOL_SIZE"`
	MinIdleConns int           `yaml:"min_idle_conns" env:"VALKEY_MIN_IDLE_CONNS"`
	MaxRetries   int           `yaml:"max_retries" env:"VALKEY_MAX_RETRIES"`
	DialTimeout  time.Duration `yaml:"dial_timeout" env:"VALKEY_DIAL_TIMEOUT"`
	ReadTimeout  time.Duration `yaml:"read_timeout" env:"VALKEY_READ_TIMEOUT"`
	WriteTimeout time.Duration `yaml:"write_timeout" env:"VALKEY_WRITE_TIMEOUT"`
	DefaultTTL   time.Duration `yaml:"default_ttl" env:"VALKEY_DEFAULT_TTL"`

	// Distributed locking configuration (cache stampede prevention)
	LockTTL     time.Duration `yaml:"lock_ttl" env:"VALKEY_LOCK_TTL"`           // TTL for distributed locks (default: 10s)
	LockMaxWait time.Duration `yaml:"lock_max_wait" env:"VALKEY_LOCK_MAX_WAIT"` // Max time to wait for lock holder (default: 3s)
}

// LoadFromEnv implements Config interface for ValkeyConfig
func (c *ValkeyConfig) LoadFromEnv() error {
	if err := env.Parse(c); err != nil {
		return fmt.Errorf("failed to parse Valkey config from env: %w", err)
	}
	return nil
}

// Client creates and returns a Valkey client from the Valkey configuration.
func (c *ValkeyConfig) Client() (valkeygo.Client, error) {
	return valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{c.Addr},
		Password:    c.Password,
		SelectDB:    c.DB,
	})
}

// Stats represents cache statistics
type Stats struct {
	Type      string `json:"type"`
	Connected bool   `json:"connected"`
	Info      string `json:"info"`
}
