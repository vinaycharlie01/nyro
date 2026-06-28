//go:build integration
// +build integration

package dragonfly_integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestDragonflyIntegration_Exists tests the Exists operation.
func TestDragonflyIntegration_Exists(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
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

// TestDragonflyIntegration_Clear tests the Clear operation.
func TestDragonflyIntegration_Clear(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
	ctx := context.Background()

	t.Run("clear_all_keys", func(t *testing.T) {
		keys := []string{"test:clear:1", "test:clear:2", "test:clear:3"}

		for _, key := range keys {
			err := cacheAdapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}

		err := cacheAdapter.Clear(ctx)
		require.NoError(t, err)

		for _, key := range keys {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})
}

// TestDragonflyIntegration_HealthCheck tests the HealthCheck operation.
func TestDragonflyIntegration_HealthCheck(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
	ctx := context.Background()

	t.Run("health_check_healthy", func(t *testing.T) {
		err := cacheAdapter.HealthCheck(ctx)
		assert.NoError(t, err)
	})
}

// TestDragonflyIntegration_GetStats tests the GetStats operation.
func TestDragonflyIntegration_GetStats(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_stats", func(t *testing.T) {
		stats, err := cacheAdapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, "dragonfly", stats.Type)
		assert.True(t, stats.Connected)
	})
}
