package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create test config
func testConfig(prefix string, ttl time.Duration, enabled bool) EntityCacheConfig {
	return EntityCacheConfig{
		KeyPrefix: prefix,
		TTL:       ttl,
		Enabled:   enabled,
	}
}

func TestEntityConfigManager_BasicOperations(t *testing.T) {
	manager := NewEntityConfigManager(map[string]EntityCacheConfig{
		"country": testConfig("country", 10*time.Minute, true),
	})

	t.Run("get existing config", func(t *testing.T) {
		config, err := manager.GetConfig("country")
		require.NoError(t, err)
		assert.Equal(t, "country", config.KeyPrefix)
		assert.Equal(t, 10*time.Minute, config.TTL)
		assert.True(t, config.Enabled)
	})

	t.Run("get non-existing config", func(t *testing.T) {
		_, err := manager.GetConfig("invalid")
		assert.Error(t, err)
	})

	t.Run("set and get config", func(t *testing.T) {
		manager.SetConfig("user", testConfig("user", 5*time.Minute, false))
		config, err := manager.GetConfig("user")
		require.NoError(t, err)
		assert.Equal(t, "user", config.KeyPrefix)
	})

	t.Run("get all configs", func(t *testing.T) {
		all := manager.GetAllConfigs()
		assert.Len(t, all, 2) // country + user
		assert.Contains(t, all, "country")
		assert.Contains(t, all, "user")
	})
}

func TestEntityConfigManager_MustGetConfig(t *testing.T) {
	manager := NewEntityConfigManager(map[string]EntityCacheConfig{
		"country": testConfig("country", 10*time.Minute, true),
	})

	t.Run("success", func(t *testing.T) {
		config := manager.MustGetConfig("country")
		assert.Equal(t, "country", config.KeyPrefix)
	})

	t.Run("panic on missing", func(t *testing.T) {
		assert.Panics(t, func() {
			manager.MustGetConfig("invalid")
		})
	})
}

func TestEntityConfigManager_LoadFromYAML(t *testing.T) {
	manager := NewEntityConfigManager(map[string]EntityCacheConfig{
		"country": testConfig("country", 30*time.Minute, true),
	})

	yamlData := []byte(`
cache:
  entities:
    country:
      key_prefix: "country_v2"
      ttl: 1h
      enabled: false
    user:
      key_prefix: "user"
      ttl: 15m
      enabled: true
`)

	err := manager.LoadFromYAML(yamlData)
	require.NoError(t, err)

	// Check updated config
	country, _ := manager.GetConfig("country")
	assert.Equal(t, "country_v2", country.KeyPrefix)
	assert.Equal(t, 1*time.Hour, country.TTL)
	assert.False(t, country.Enabled)

	// Check new config
	user, _ := manager.GetConfig("user")
	assert.Equal(t, "user", user.KeyPrefix)
	assert.Equal(t, 15*time.Minute, user.TTL)
	assert.True(t, user.Enabled)
}

func TestEntityConfigManager_LoadFromYAML_Invalid(t *testing.T) {
	manager := NewEntityConfigManager(nil)
	err := manager.LoadFromYAML([]byte(`invalid: yaml: [[[`))
	assert.Error(t, err)
}

func TestEntityConfigManager_LoadFromEnv(t *testing.T) {
	// Setup env vars - prefix must match entity name exactly
	os.Setenv("CACHE_country_KEY_PREFIX", "country_env")
	os.Setenv("CACHE_country_TTL", "45m")
	os.Setenv("CACHE_country_ENABLED", "false")
	defer func() {
		os.Unsetenv("CACHE_country_KEY_PREFIX")
		os.Unsetenv("CACHE_country_TTL")
		os.Unsetenv("CACHE_country_ENABLED")
	}()

	manager := NewEntityConfigManager(map[string]EntityCacheConfig{
		"country": testConfig("country", 30*time.Minute, true),
	})

	err := manager.LoadFromEnv()
	require.NoError(t, err)

	config, _ := manager.GetConfig("country")
	assert.Equal(t, "country_env", config.KeyPrefix)
	assert.Equal(t, 45*time.Minute, config.TTL)
	assert.False(t, config.Enabled)
}

func TestLoadEntityConfigManager(t *testing.T) {
	yamlData := []byte(`
cache:
  entities:
    country:
      key_prefix: "country"
      ttl: 1h
      enabled: true
`)

	manager := LoadEntityConfigManager(yamlData)
	require.NotNil(t, manager)

	config, err := manager.GetConfig("country")
	require.NoError(t, err)
	assert.Equal(t, "country", config.KeyPrefix)
	assert.Equal(t, 1*time.Hour, config.TTL)
	assert.True(t, config.Enabled)
}

func TestToEnvFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"country", "COUNTRY"},
		{"user_group", "USER_GROUP"},
		{"api-endpoint", "API_ENDPOINT"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, toEnvFormat(tt.input))
	}
}
