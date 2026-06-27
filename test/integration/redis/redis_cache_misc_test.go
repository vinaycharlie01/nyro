//go:build integration
// +build integration

package redis_integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestRedisIntegration_Exists tests the Exists operation
func TestRedisIntegration_Exists(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("exists_for_existing_key", func(t *testing.T) {
		key := "test:exists:present"
		value := "exists-value"

		err := cacheAdapter.Set(ctx, key, value)
		require.NoError(t, err)

		exists, err := cacheAdapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("exists_for_non_existing_key", func(t *testing.T) {
		key := "test:exists:absent"

		exists, err := cacheAdapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

// TestRedisIntegration_Clear tests the Clear operation
func TestRedisIntegration_Clear(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("clear_all_keys", func(t *testing.T) {
		// Set multiple keys
		keys := []string{"test:clear:1", "test:clear:2", "test:clear:3"}
		for _, key := range keys {
			err := cacheAdapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}

		// Clear all
		err := cacheAdapter.Clear(ctx)
		require.NoError(t, err)

		// Verify all keys are gone
		for _, key := range keys {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})
}

// TestRedisIntegration_HealthCheck tests the HealthCheck operation
func TestRedisIntegration_HealthCheck(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("health_check_healthy", func(t *testing.T) {
		err := cacheAdapter.HealthCheck(ctx)
		assert.NoError(t, err)
	})
}

// TestRedisIntegration_GetStats tests the GetStats operation
func TestRedisIntegration_GetStats(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_stats", func(t *testing.T) {
		stats, err := cacheAdapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, "redis", stats.Type)
		assert.True(t, stats.Connected)
	})
}
