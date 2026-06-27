//go:build integration
// +build integration

package redis_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestRedisIntegration_GetMulti tests the GetMulti operation
func TestRedisIntegration_GetMulti(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_multi_all_existing", func(t *testing.T) {
		keys := []any{"test:multi:1", "test:multi:2", "test:multi:3"}
		values := map[any]any{
			"test:multi:1": "value1",
			"test:multi:2": "value2",
			"test:multi:3": "value3",
		}

		// Set all values
		for key, val := range values {
			err := cacheAdapter.Set(ctx, key, val)
			require.NoError(t, err)
		}

		// Get multiple values
		result, err := cacheAdapter.GetMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, values, result)
	})

	t.Run("get_multi_partial_existing", func(t *testing.T) {
		key1 := "test:multi:partial:1"
		key2 := "test:multi:partial:2"
		key3 := "test:multi:partial:3"

		// Set only first two keys
		err := cacheAdapter.Set(ctx, key1, "value1")
		require.NoError(t, err)
		err = cacheAdapter.Set(ctx, key2, "value2")
		require.NoError(t, err)

		// Get all three keys
		result, err := cacheAdapter.GetMulti(ctx, []any{key1, key2, key3})
		require.NoError(t, err)
		assert.Len(t, result, 2) // Only 2 keys exist
		assert.Equal(t, "value1", result[key1])
		assert.Equal(t, "value2", result[key2])
	})

	t.Run("get_multi_empty_keys", func(t *testing.T) {
		result, err := cacheAdapter.GetMulti(ctx, []any{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

// TestRedisIntegration_SetMulti tests the SetMulti operation
func TestRedisIntegration_SetMulti(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("set_multi_multiple_keys", func(t *testing.T) {
		items := map[any]any{
			"test:setmulti:1": "value1",
			"test:setmulti:2": "value2",
			"test:setmulti:3": "value3",
		}

		err := cacheAdapter.SetMulti(ctx, items)
		require.NoError(t, err)

		// Verify all values
		for key, expectedVal := range items {
			val, err := cacheAdapter.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedVal, val)
		}
	})

	t.Run("set_multi_with_ttl", func(t *testing.T) {
		items := map[any]any{
			"test:setmulti:ttl:1": "value1",
			"test:setmulti:ttl:2": "value2",
		}

		err := cacheAdapter.SetMulti(ctx, items, cache.WithTTL(1*time.Second))
		require.NoError(t, err)

		// Values should exist immediately
		for key := range items {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		// Wait for expiration
		time.Sleep(1500 * time.Millisecond)

		// Values should be expired
		for key := range items {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("set_multi_empty_items", func(t *testing.T) {
		err := cacheAdapter.SetMulti(ctx, map[any]any{})
		assert.NoError(t, err)
	})
}

// TestRedisIntegration_DeleteMulti tests the DeleteMulti operation
func TestRedisIntegration_DeleteMulti(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("delete_multi_existing_keys", func(t *testing.T) {
		keys := []any{"test:delmulti:1", "test:delmulti:2", "test:delmulti:3"}

		// Set all keys
		for _, key := range keys {
			err := cacheAdapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}

		// Delete all keys
		err := cacheAdapter.DeleteMulti(ctx, keys)
		require.NoError(t, err)

		// Verify deletion
		for _, key := range keys {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("delete_multi_empty_keys", func(t *testing.T) {
		err := cacheAdapter.DeleteMulti(ctx, []any{})
		assert.NoError(t, err)
	})
}
