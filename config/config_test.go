package config_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/vinaycharlie01/nyro/adapters/redis"
	"github.com/vinaycharlie01/nyro/config"
)

func TestRedisConfig_LoadFromEnv(t *testing.T) {
	t.Run("success_with_env_vars", func(t *testing.T) {
		t.Setenv("REDIS_ADDR", "localhost:6379")
		t.Setenv("REDIS_DB", "1")
		t.Setenv("REDIS_DEFAULT_TTL", "1h")

		cfg := &config.RedisConfig{}
		err := cfg.LoadFromEnv()

		require.NoError(t, err)
		assert.Equal(t, "localhost:6379", cfg.Addr)
		assert.Equal(t, 1, cfg.DB)
		assert.Equal(t, 1*time.Hour, cfg.DefaultTTL)
	})

	t.Run("success_without_env_vars", func(t *testing.T) {
		cfg := &config.RedisConfig{}
		err := cfg.LoadFromEnv()

		require.NoError(t, err)
		assert.Equal(t, "", cfg.Addr)
		assert.Equal(t, 0, cfg.DB)
	})

	t.Run("error_invalid_int", func(t *testing.T) {
		t.Setenv("REDIS_DB", "invalid")

		cfg := &config.RedisConfig{}
		err := cfg.LoadFromEnv()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse Redis config from env")
	})

	t.Run("error_invalid_duration", func(t *testing.T) {
		t.Setenv("REDIS_DIAL_TIMEOUT", "invalid")

		cfg := &config.RedisConfig{}
		err := cfg.LoadFromEnv()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse Redis config from env")
	})
}

func TestNew(t *testing.T) {
	mockRedis := miniredis.RunT(t)

	t.Run("success", func(t *testing.T) {
		t.Setenv("REDIS_ADDR", mockRedis.Addr())

		c, err := config.New(config.CacheRedis, &config.RedisConfig{})

		require.NoError(t, err)
		assert.NotNil(t, c)
		defer func() {
			_ = c.Close()
		}()
	})

	t.Run("error_unsupported_type", func(t *testing.T) {
		c, err := config.New(config.CacheType("invalid"), &config.RedisConfig{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported cache type")
		assert.Nil(t, c)
	})

	t.Run("error_config_load", func(t *testing.T) {
		t.Setenv("REDIS_DB", "invalid")

		c, err := config.New(config.CacheRedis, &config.RedisConfig{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load config from env")
		assert.Nil(t, c)
	})
}
