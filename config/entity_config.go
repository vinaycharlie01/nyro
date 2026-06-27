package config

import (
	"fmt"
	"strings"
	"sync"
	"time"

	slogger "log/slog"

	"github.com/caarlos0/env/v11"
	"gopkg.in/yaml.v3"
)

// LoadEntityConfigManager creates a new EntityConfigManager from YAML bytes.
// Environment variables take highest priority and override YAML configuration.
// This is the recommended way to initialize the config manager.
func LoadEntityConfigManager(yamlData []byte) *EntityConfigManager {
	manager := &EntityConfigManager{
		entities: make(map[string]EntityCacheConfig),
	}
	if len(yamlData) > 0 {
		if err := manager.LoadFromYAML(yamlData); err != nil {
			slogger.Warn("failed to load cache config from YAML", "error", err)
		}
	}
	if err := manager.LoadFromEnv(); err != nil {
		slogger.Warn("failed to load cache config from environment", "error", err)
	}
	return manager
}

// toEnvFormat converts entity name to environment variable format
// Example: "user_group" -> "USER_GROUP"
func toEnvFormat(s string) string {
	return strings.ToUpper(strings.ReplaceAll(s, "-", "_"))
}

// EntityCacheConfig represents cache configuration for a specific entity/repository.
// Configuration priority: Environment Variables > YAML
type EntityCacheConfig struct {
	KeyPrefix string        `yaml:"key_prefix" env:"KEY_PREFIX"`
	TTL       time.Duration `yaml:"ttl" env:"TTL"`
	Enabled   bool          `yaml:"enabled" env:"ENABLED"`
}

// EntityConfigManager manages cache configurations for entities.
// Thread-safe for concurrent reads and potential runtime updates.
type EntityConfigManager struct {
	mu       sync.RWMutex
	entities map[string]EntityCacheConfig
}

// NewEntityConfigManager creates a new entity config manager with optional default configurations.
// Use LoadEntityConfigManager to load from YAML and environment variables.
func NewEntityConfigManager(defaults ...map[string]EntityCacheConfig) *EntityConfigManager {
	manager := &EntityConfigManager{
		entities: make(map[string]EntityCacheConfig),
	}

	// Load defaults if provided
	if len(defaults) > 0 && defaults[0] != nil {
		for name, config := range defaults[0] {
			manager.entities[name] = config
		}
	}

	return manager
}

// LoadFromYAML loads entity cache configuration from YAML data
func (m *EntityConfigManager) LoadFromYAML(data []byte) error {
	var wrapper struct {
		Cache struct {
			Entities map[string]EntityCacheConfig `yaml:"entities"`
		} `yaml:"cache"`
	}

	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return fmt.Errorf("failed to unmarshal YAML config: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Merge with existing config
	for name, config := range wrapper.Cache.Entities {
		if existing, ok := m.entities[name]; ok {
			// Merge non-zero values
			if config.KeyPrefix != "" {
				existing.KeyPrefix = config.KeyPrefix
			}
			if config.TTL != 0 {
				existing.TTL = config.TTL
			}

			existing.Enabled = config.Enabled
			m.entities[name] = existing
		} else {
			m.entities[name] = config
		}
	}

	return nil
}

// LoadFromEnv loads cache configuration from environment variables using github.com/caarlos0/env.
// Environment variables follow the pattern: CACHE_{ENTITY}_{FIELD}
// Examples:
//   - CACHE_COUNTRY_KEY_PREFIX=country_v2
//   - CACHE_COUNTRY_TTL=1h
//   - CACHE_COUNTRY_ENABLED=true
func (m *EntityConfigManager) LoadFromEnv() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for entityName, config := range m.entities {
		// Parse environment variables with prefix CACHE_{ENTITY}_
		envPrefix := fmt.Sprintf("CACHE_%s_", entityName)
		if err := env.ParseWithOptions(&config, env.Options{
			Prefix: envPrefix,
		}); err != nil {
			slogger.Debug("failed to parse environment variables for entity", "entity", entityName, "prefix", envPrefix, "error", err)
			continue
		}
		m.entities[entityName] = config
	}

	return nil
}

// GetConfig returns the cache configuration for a specific entity
func (m *EntityConfigManager) GetConfig(entityName string) (EntityCacheConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	config, ok := m.entities[entityName]
	if !ok {
		return EntityCacheConfig{}, fmt.Errorf("entity config not found: %s", entityName)
	}
	return config, nil
}

// MustGetConfig returns the cache configuration for a specific entity or panics
func (m *EntityConfigManager) MustGetConfig(entityName string) EntityCacheConfig {
	config, err := m.GetConfig(entityName)
	if err != nil {
		panic(err)
	}
	return config
}

// GetAllConfigs returns a copy of all entity cache configurations
func (m *EntityConfigManager) GetAllConfigs() map[string]EntityCacheConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := make(map[string]EntityCacheConfig, len(m.entities))
	for name, config := range m.entities {
		configs[name] = config
	}
	return configs
}

// SetConfig sets or updates the cache configuration for a specific entity
func (m *EntityConfigManager) SetConfig(entityName string, config EntityCacheConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entities[entityName] = config
}

// RegisterEntity registers a new entity with its cache configuration
// This is an alias for SetConfig for better semantic clarity
func (m *EntityConfigManager) RegisterEntity(entityName string, config EntityCacheConfig) {
	m.SetConfig(entityName, config)
}
