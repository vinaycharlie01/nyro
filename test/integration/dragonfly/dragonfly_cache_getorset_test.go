//go:build integration
// +build integration

package dragonfly_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestDragonflyIntegration_GetOrSet tests the GetOrSet operation.
func TestDragonflyIntegration_GetOrSet(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_or_set_cache_miss", func(t *testing.T) {
		key := "test:getorset:miss"
		loaderCalls := 0

		loader := func(_ context.Context) (any, error) {
			loaderCalls++

			return "loaded-value", nil
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader, cache.WithTTL(5*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, "loaded-value", val)
		assert.Equal(t, 1, loaderCalls)
	})

	t.Run("get_or_set_cache_hit", func(t *testing.T) {
		key := "test:getorset:hit"
		loaderCalls := 0

		err := cacheAdapter.Set(ctx, key, "cached-value")
		require.NoError(t, err)

		loader := func(_ context.Context) (any, error) {
			loaderCalls++

			return "new-value", nil
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader, cache.WithTTL(5*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, "cached-value", val)
		assert.Equal(t, 0, loaderCalls)
	})
}
