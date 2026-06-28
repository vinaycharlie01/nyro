//go:build integration
// +build integration

package valkey_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestValkeyIntegration_SetGet tests basic Set and Get operations.
func TestValkeyIntegration_SetGet(t *testing.T) {
	valkey := testcontainers.SetupValkeyContainer(t)
	cacheAdapter := valkey.GetCacheAdapter()
	ctx := context.Background()

	t.Run("set_simple_value", func(t *testing.T) {
		key := "test:set:simple"
		value := "simple-value"

		err := cacheAdapter.Set(ctx, key, value)
		require.NoError(t, err)

		val, err := cacheAdapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, val)
	})

	t.Run("set_with_ttl", func(t *testing.T) {
		key := "test:set:ttl"
		value := "ttl-value"

		err := cacheAdapter.Set(ctx, key, value, cache.WithTTL(1*time.Second))
		require.NoError(t, err)

		val, err := cacheAdapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, val)

		time.Sleep(1500 * time.Millisecond)

		_, err = cacheAdapter.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("set_overwrite_existing", func(t *testing.T) {
		key := "test:set:overwrite"
		value1 := "original-value"
		value2 := "updated-value"

		err := cacheAdapter.Set(ctx, key, value1)
		require.NoError(t, err)

		err = cacheAdapter.Set(ctx, key, value2)
		require.NoError(t, err)

		val, err := cacheAdapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value2, val)
	})

	t.Run("set_complex_types", func(t *testing.T) {
		type User struct {
			ID   int
			Name string
		}

		key := "test:set:complex"
		user := User{ID: 1, Name: "John"}

		err := cacheAdapter.Set(ctx, key, user)
		require.NoError(t, err)

		val, err := cacheAdapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[User](val)
		require.NoError(t, err)
		assert.Equal(t, user, decoded)
	})
}

// TestValkeyIntegration_Delete tests the Delete operation.
func TestValkeyIntegration_Delete(t *testing.T) {
	valkey := testcontainers.SetupValkeyContainer(t)
	cacheAdapter := valkey.GetCacheAdapter()
	ctx := context.Background()

	t.Run("delete_existing_key", func(t *testing.T) {
		key := "test:delete:existing"
		err := cacheAdapter.Set(ctx, key, "value")
		require.NoError(t, err)

		err = cacheAdapter.Delete(ctx, key)
		require.NoError(t, err)

		exists, err := cacheAdapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("delete_non_existing_key", func(t *testing.T) {
		key := "test:delete:missing"

		err := cacheAdapter.Delete(ctx, key)
		assert.NoError(t, err)
	})
}

// TestValkeyIntegration_Misc tests Exists, Clear, HealthCheck, GetStats.
func TestValkeyIntegration_Misc(t *testing.T) {
	valkey := testcontainers.SetupValkeyContainer(t)
	cacheAdapter := valkey.GetCacheAdapter()
	ctx := context.Background()

	t.Run("exists_for_existing_key", func(t *testing.T) {
		key := "test:exists:present"
		err := cacheAdapter.Set(ctx, key, "value")
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

	t.Run("health_check_healthy", func(t *testing.T) {
		err := cacheAdapter.HealthCheck(ctx)
		assert.NoError(t, err)
	})

	t.Run("get_stats", func(t *testing.T) {
		stats, err := cacheAdapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, "valkey", stats.Type)
		assert.True(t, stats.Connected)
	})
}

// TestValkeyIntegration_GetOrSet tests the GetOrSet operation.
func TestValkeyIntegration_GetOrSet(t *testing.T) {
	valkey := testcontainers.SetupValkeyContainer(t)
	cacheAdapter := valkey.GetCacheAdapter()
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

// TestValkeyIntegration_MultiOperations tests GetMulti, SetMulti, DeleteMulti.
func TestValkeyIntegration_MultiOperations(t *testing.T) {
	valkey := testcontainers.SetupValkeyContainer(t)
	cacheAdapter := valkey.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_multi_all_existing", func(t *testing.T) {
		keys := []any{"test:multi:1", "test:multi:2", "test:multi:3"}
		values := map[any]any{
			"test:multi:1": "value1",
			"test:multi:2": "value2",
			"test:multi:3": "value3",
		}

		for key, val := range values {
			err := cacheAdapter.Set(ctx, key, val)
			require.NoError(t, err)
		}

		result, err := cacheAdapter.GetMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, values, result)
	})

	t.Run("get_multi_partial_existing", func(t *testing.T) {
		key1 := "test:multi:partial:1"
		key2 := "test:multi:partial:2"
		key3 := "test:multi:partial:3"

		err := cacheAdapter.Set(ctx, key1, "value1")
		require.NoError(t, err)
		err = cacheAdapter.Set(ctx, key2, "value2")
		require.NoError(t, err)

		result, err := cacheAdapter.GetMulti(ctx, []any{key1, key2, key3})
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "value1", result[key1])
		assert.Equal(t, "value2", result[key2])
	})

	t.Run("get_multi_empty_keys", func(t *testing.T) {
		result, err := cacheAdapter.GetMulti(ctx, []any{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("set_multi_multiple_keys", func(t *testing.T) {
		items := map[any]any{
			"test:setmulti:1": "value1",
			"test:setmulti:2": "value2",
			"test:setmulti:3": "value3",
		}

		err := cacheAdapter.SetMulti(ctx, items)
		require.NoError(t, err)

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

		for key := range items {
			exists, err := cacheAdapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		time.Sleep(1500 * time.Millisecond)

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

	t.Run("delete_multi_existing_keys", func(t *testing.T) {
		keys := []any{"test:delmulti:1", "test:delmulti:2", "test:delmulti:3"}

		for _, key := range keys {
			err := cacheAdapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}

		err := cacheAdapter.DeleteMulti(ctx, keys)
		require.NoError(t, err)

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
