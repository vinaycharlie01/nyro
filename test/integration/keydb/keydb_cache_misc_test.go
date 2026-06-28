//go:build integration
// +build integration

package keydb_integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestKeyDBIntegration_Exists tests the Exists operation.
func TestKeyDBIntegration_Exists(t *testing.T) {
	keydb := testcontainers.SetupKeyDBContainer(t)
	cacheAdapter := keydb.GetCacheAdapter()
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

// TestKeyDBIntegration_Clear tests the Clear operation.
func TestKeyDBIntegration_Clear(t *testing.T) {
	keydb := testcontainers.SetupKeyDBContainer(t)
	cacheAdapter := keydb.GetCacheAdapter()
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

// TestKeyDBIntegration_HealthCheck tests the HealthCheck operation.
func TestKeyDBIntegration_HealthCheck(t *testing.T) {
	keydb := testcontainers.SetupKeyDBContainer(t)
	cacheAdapter := keydb.GetCacheAdapter()
	ctx := context.Background()

	t.Run("health_check_healthy", func(t *testing.T) {
		err := cacheAdapter.HealthCheck(ctx)
		assert.NoError(t, err)
	})
}

// TestKeyDBIntegration_GetStats tests the GetStats operation.
func TestKeyDBIntegration_GetStats(t *testing.T) {
	keydb := testcontainers.SetupKeyDBContainer(t)
	cacheAdapter := keydb.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_stats", func(t *testing.T) {
		stats, err := cacheAdapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, "keydb", stats.Type)
		assert.True(t, stats.Connected)
	})
}
