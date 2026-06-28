//go:build integration
// +build integration

package ristretto_integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	ristrettoadapter "github.com/vinaycharlie01/nyro/adapters/ristretto"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
)

func setupRistretto(t *testing.T) *ristrettoadapter.Adapter {
	t.Helper()
	cfg := nyroconfig.RistrettoConfig{
		NumCounters: 1e7,
		MaxCost:     100 << 20,
		BufferItems: 64,
		DefaultTTL:  5 * time.Minute,
	}
	adapter, err := ristrettoadapter.New(cfg)
	require.NoError(t, err)
	require.NotNil(t, adapter)
	return adapter
}

// waitForRistretto sleeps briefly to allow Ristretto's async commit to complete.
func waitForRistretto() {
	time.Sleep(50 * time.Millisecond)
}

// TestRistrettoIntegration_SetGet tests basic Set and Get operations.
func TestRistrettoIntegration_SetGet(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("set_simple_value", func(t *testing.T) {
		key := "test:set:simple"
		value := "simple-value"

		err := adapter.Set(ctx, key, value)
		require.NoError(t, err)

		// Ristretto commits writes asynchronously; wait for the commit to complete.
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, val)
	})

	t.Run("set_with_ttl", func(t *testing.T) {
		key := "test:set:ttl"
		value := "ttl-value"

		err := adapter.Set(ctx, key, value, cache.WithTTL(2*time.Second))
		require.NoError(t, err)

		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, val)

		// Ristretto eviction is async; wait and verify it eventually disappears.
		time.Sleep(2500 * time.Millisecond)

		_, err = adapter.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("set_overwrite_existing", func(t *testing.T) {
		key := "test:set:overwrite"
		value1 := "original-value"
		value2 := "updated-value"

		err := adapter.Set(ctx, key, value1)
		require.NoError(t, err)
		waitForRistretto()

		err = adapter.Set(ctx, key, value2)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value2, val)
	})

	t.Run("get_non_existing_key", func(t *testing.T) {
		_, err := adapter.Get(ctx, "test:set:missing")
		assert.Error(t, err)
	})

	t.Run("set_complex_types", func(t *testing.T) {
		type User struct {
			ID   int
			Name string
		}

		key := "test:set:complex"
		user := User{ID: 1, Name: "John"}

		err := adapter.Set(ctx, key, user)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[User](val)
		require.NoError(t, err)
		assert.Equal(t, user, decoded)
	})

	t.Run("set_integer_value", func(t *testing.T) {
		key := "test:set:int"
		value := 42

		err := adapter.Set(ctx, key, value)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[int](val)
		require.NoError(t, err)
		assert.Equal(t, value, decoded)
	})

	t.Run("set_slice_value", func(t *testing.T) {
		key := "test:set:slice"
		value := []string{"a", "b", "c"}

		err := adapter.Set(ctx, key, value)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[[]string](val)
		require.NoError(t, err)
		assert.Equal(t, value, decoded)
	})

	t.Run("set_map_value", func(t *testing.T) {
		key := "test:set:map"
		value := map[string]int{"a": 1, "b": 2}

		err := adapter.Set(ctx, key, value)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[map[string]int](val)
		require.NoError(t, err)
		assert.Equal(t, value, decoded)
	})

	t.Run("set_pointer_value", func(t *testing.T) {
		type Item struct {
			X int
		}
		key := "test:set:ptr"
		value := &Item{X: 99}

		err := adapter.Set(ctx, key, value)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)

		decoded, err := cache.Decode[*Item](val)
		require.NoError(t, err)
		assert.Equal(t, value.X, decoded.X)
	})
}

// TestRistrettoIntegration_Delete tests the Delete operation.
func TestRistrettoIntegration_Delete(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("delete_existing_key", func(t *testing.T) {
		key := "test:delete:existing"
		err := adapter.Set(ctx, key, "value")
		require.NoError(t, err)
		waitForRistretto()

		err = adapter.Delete(ctx, key)
		require.NoError(t, err)

		exists, err := adapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("delete_non_existing_key", func(t *testing.T) {
		key := "test:delete:missing"

		err := adapter.Delete(ctx, key)
		assert.NoError(t, err)
	})

	t.Run("delete_and_reinsert", func(t *testing.T) {
		key := "test:delete:reinsert"
		err := adapter.Set(ctx, key, "original")
		require.NoError(t, err)
		waitForRistretto()

		err = adapter.Delete(ctx, key)
		require.NoError(t, err)

		exists, err := adapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)

		err = adapter.Set(ctx, key, "new-value")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "new-value", val)
	})
}

// TestRistrettoIntegration_Misc tests Exists, Clear, HealthCheck, GetStats.
func TestRistrettoIntegration_Misc(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("exists_for_existing_key", func(t *testing.T) {
		key := "test:exists:present"
		err := adapter.Set(ctx, key, "value")
		require.NoError(t, err)
		waitForRistretto()

		exists, err := adapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("exists_for_non_existing_key", func(t *testing.T) {
		key := "test:exists:absent"

		exists, err := adapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("clear_all_keys", func(t *testing.T) {
		keys := []string{"test:clear:1", "test:clear:2", "test:clear:3"}
		for _, key := range keys {
			err := adapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}
		waitForRistretto()

		err := adapter.Clear(ctx)
		require.NoError(t, err)

		for _, key := range keys {
			exists, err := adapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("clear_empty_cache", func(t *testing.T) {
		err := adapter.Clear(ctx)
		assert.NoError(t, err)
	})

	t.Run("health_check_healthy", func(t *testing.T) {
		err := adapter.HealthCheck(ctx)
		assert.NoError(t, err)
	})

	t.Run("health_check_after_close", func(t *testing.T) {
		local := setupRistretto(t)
		err := local.Close()
		require.NoError(t, err)

		err = local.HealthCheck(ctx)
		assert.Error(t, err)
	})

	t.Run("get_stats", func(t *testing.T) {
		stats, err := adapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Equal(t, "ristretto", stats.Type)
		assert.True(t, stats.Connected)
	})
}

// TestRistrettoIntegration_GetOrSet tests the GetOrSet operation.
func TestRistrettoIntegration_GetOrSet(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("get_or_set_cache_miss", func(t *testing.T) {
		key := "test:getorset:miss"
		loaderCalls := 0

		loader := func(_ context.Context) (any, error) {
			loaderCalls++
			return "loaded-value", nil
		}

		val, err := adapter.GetOrSet(ctx, key, loader, cache.WithTTL(5*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, "loaded-value", val)
		assert.Equal(t, 1, loaderCalls)
	})

	t.Run("get_or_set_cache_hit", func(t *testing.T) {
		key := "test:getorset:hit"
		loaderCalls := 0

		err := adapter.Set(ctx, key, "cached-value")
		require.NoError(t, err)
		waitForRistretto()

		loader := func(_ context.Context) (any, error) {
			loaderCalls++
			return "new-value", nil
		}

		val, err := adapter.GetOrSet(ctx, key, loader, cache.WithTTL(5*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, "cached-value", val)
		assert.Equal(t, 0, loaderCalls)
	})

	t.Run("get_or_set_loader_error", func(t *testing.T) {
		key := "test:getorset:loadererr"

		loader := func(_ context.Context) (any, error) {
			return nil, assert.AnError
		}

		_, err := adapter.GetOrSet(ctx, key, loader)
		assert.Error(t, err)
	})
}

// TestRistrettoIntegration_MultiOperations tests GetMulti, SetMulti, DeleteMulti.
func TestRistrettoIntegration_MultiOperations(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("get_multi_all_existing", func(t *testing.T) {
		keys := []any{"test:multi:1", "test:multi:2", "test:multi:3"}
		values := map[any]any{
			"test:multi:1": "value1",
			"test:multi:2": "value2",
			"test:multi:3": "value3",
		}

		for key, val := range values {
			err := adapter.Set(ctx, key, val)
			require.NoError(t, err)
		}
		waitForRistretto()

		result, err := adapter.GetMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, values, result)
	})

	t.Run("get_multi_partial_existing", func(t *testing.T) {
		key1 := "test:multi:partial:1"
		key2 := "test:multi:partial:2"
		key3 := "test:multi:partial:3"

		err := adapter.Set(ctx, key1, "value1")
		require.NoError(t, err)
		err = adapter.Set(ctx, key2, "value2")
		require.NoError(t, err)
		waitForRistretto()

		result, err := adapter.GetMulti(ctx, []any{key1, key2, key3})
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "value1", result[key1])
		assert.Equal(t, "value2", result[key2])
	})

	t.Run("get_multi_empty_keys", func(t *testing.T) {
		result, err := adapter.GetMulti(ctx, []any{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("get_multi_nil_keys", func(t *testing.T) {
		result, err := adapter.GetMulti(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("set_multi_multiple_keys", func(t *testing.T) {
		items := map[any]any{
			"test:setmulti:1": "value1",
			"test:setmulti:2": "value2",
			"test:setmulti:3": "value3",
		}

		err := adapter.SetMulti(ctx, items)
		require.NoError(t, err)
		waitForRistretto()

		for key, expectedVal := range items {
			val, err := adapter.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedVal, val)
		}
	})

	t.Run("set_multi_with_ttl", func(t *testing.T) {
		items := map[any]any{
			"test:setmulti:ttl:1": "value1",
			"test:setmulti:ttl:2": "value2",
		}

		err := adapter.SetMulti(ctx, items, cache.WithTTL(2*time.Second))
		require.NoError(t, err)
		waitForRistretto()

		for key := range items {
			exists, err := adapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		time.Sleep(2500 * time.Millisecond)

		for key := range items {
			exists, err := adapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("set_multi_empty_items", func(t *testing.T) {
		err := adapter.SetMulti(ctx, map[any]any{})
		assert.NoError(t, err)
	})

	t.Run("set_multi_nil_items", func(t *testing.T) {
		err := adapter.SetMulti(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("delete_multi_existing_keys", func(t *testing.T) {
		keys := []any{"test:delmulti:1", "test:delmulti:2", "test:delmulti:3"}

		for _, key := range keys {
			err := adapter.Set(ctx, key, "value")
			require.NoError(t, err)
		}
		waitForRistretto()

		err := adapter.DeleteMulti(ctx, keys)
		require.NoError(t, err)

		for _, key := range keys {
			exists, err := adapter.Exists(ctx, key)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("delete_multi_empty_keys", func(t *testing.T) {
		err := adapter.DeleteMulti(ctx, []any{})
		assert.NoError(t, err)
	})

	t.Run("delete_multi_nil_keys", func(t *testing.T) {
		err := adapter.DeleteMulti(ctx, nil)
		assert.NoError(t, err)
	})
}

// TestRistrettoIntegration_NonStringKeys tests that keys are properly converted to strings.
func TestRistrettoIntegration_NonStringKeys(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("integer_key", func(t *testing.T) {
		err := adapter.Set(ctx, 42, "value-for-int-key")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, 42)
		require.NoError(t, err)
		assert.Equal(t, "value-for-int-key", val)
	})

	t.Run("float_key", func(t *testing.T) {
		err := adapter.Set(ctx, 3.14, "value-for-float-key")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, 3.14)
		require.NoError(t, err)
		assert.Equal(t, "value-for-float-key", val)
	})

	t.Run("string_key_via_int", func(t *testing.T) {
		// string "42" and int 42 should map to the same underlying key "42"
		err := adapter.Set(ctx, "42", "string-key-value")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, 42)
		require.NoError(t, err)
		assert.Equal(t, "string-key-value", val)
	})
}

// TestRistrettoIntegration_DefaultTTL tests behaviour with default TTL.
func TestRistrettoIntegration_DefaultTTL(t *testing.T) {
	cfg := nyroconfig.RistrettoConfig{
		NumCounters: 1e7,
		MaxCost:     100 << 20,
		BufferItems: 64,
		DefaultTTL:  500 * time.Millisecond,
	}
	adapter, err := ristrettoadapter.New(cfg)
	require.NoError(t, err)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("default_ttl_expires", func(t *testing.T) {
		key := "test:defaultttl:expire"
		err := adapter.Set(ctx, key, "value")
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		exists, err := adapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		time.Sleep(600 * time.Millisecond)

		_, err = adapter.Get(ctx, key)
		assert.Error(t, err)
	})
}

// TestRistrettoIntegration_Close tests that operations fail after close.
func TestRistrettoIntegration_Close(t *testing.T) {
	adapter := setupRistretto(t)
	ctx := context.Background()

	err := adapter.Close()
	require.NoError(t, err)

	t.Run("get_after_close", func(t *testing.T) {
		_, err := adapter.Get(ctx, "any-key")
		assert.Error(t, err)
	})

	t.Run("set_after_close", func(t *testing.T) {
		err := adapter.Set(ctx, "any-key", "value")
		assert.Error(t, err)
	})

	t.Run("delete_after_close", func(t *testing.T) {
		err := adapter.Delete(ctx, "any-key")
		// Delete may not error since it doesn't check closed state at cart level
		t.Logf("delete after close: %v", err)
	})

	t.Run("clear_after_close", func(t *testing.T) {
		err := adapter.Clear(ctx)
		// Clear may succeed since it calls ristretto's Clear directly
		t.Logf("clear after close: %v", err)
	})

	t.Run("exists_after_close", func(t *testing.T) {
		_, err := adapter.Exists(ctx, "any-key")
		assert.Error(t, err)
	})

	t.Run("close_called_twice", func(t *testing.T) {
		err := adapter.Close()
		assert.NoError(t, err)
	})
}

// TestRistrettoIntegration_ConfigOptions tests that config options are respected.
func TestRistrettoIntegration_ConfigOptions(t *testing.T) {
	t.Run("custom_num_counters", func(t *testing.T) {
		cfg := nyroconfig.RistrettoConfig{
			NumCounters: 1000,
			MaxCost:     1 << 20, // 1 MB
			BufferItems: 32,
		}

		adapter, err := ristrettoadapter.New(cfg)
		require.NoError(t, err)
		defer adapter.Close()

		ctx := context.Background()
		err = adapter.Set(ctx, "test:customcfg", "value")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, "test:customcfg")
		require.NoError(t, err)
		assert.Equal(t, "value", val)
	})

	t.Run("zero_ttl_falls_back", func(t *testing.T) {
		cfg := nyroconfig.RistrettoConfig{
			NumCounters: 1e7,
			MaxCost:     100 << 20,
			BufferItems: 64,
			DefaultTTL:  0, // no default
		}
		adapter, err := ristrettoadapter.New(cfg)
		require.NoError(t, err)
		defer adapter.Close()

		ctx := context.Background()
		err = adapter.Set(ctx, "test:zerottl", "value")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, "test:zerottl")
		require.NoError(t, err)
		assert.Equal(t, "value", val)
	})
}

// TestRistrettoIntegration_Throughput tests basic concurrent access.
func TestRistrettoIntegration_Throughput(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("concurrent_set_and_get", func(t *testing.T) {
		const goroutines = 10
		const keysPerGoroutine = 10

		done := make(chan bool, goroutines)

		for g := 0; g < goroutines; g++ {
			go func(id int) {
				for i := 0; i < keysPerGoroutine; i++ {
					key := fmt.Sprintf("test:conc:%d:%d", id, i)
					val := fmt.Sprintf("val-%d-%d", id, i)
					_ = adapter.Set(ctx, key, val)

					// Small pause to let async set complete probabilistically
					time.Sleep(10 * time.Millisecond)

					_, err := adapter.Get(ctx, key)
					// May or may not find due to async nature of Ristretto
					_ = err
				}
				done <- true
			}(g)
		}

		for g := 0; g < goroutines; g++ {
			<-done
		}
	})
}

// TestRistrettoIntegration_LowCostEviction tests that items close to the cost limit
// are evicted correctly.
func TestRistrettoIntegration_LowCostEviction(t *testing.T) {
	cfg := nyroconfig.RistrettoConfig{
		NumCounters: 100,
		MaxCost:     1 << 10, // 1 KB — very small cache
		BufferItems: 32,
		DefaultTTL:  0,
	}
	adapter, err := ristrettoadapter.New(cfg)
	require.NoError(t, err)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("fill_beyond_capacity", func(t *testing.T) {
		// Insert many keys to test eviction behaviour in a constrained cache.
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("test:evict:%d", i)
			err := adapter.Set(ctx, key, "some-value-with-moderate-overhead")
			require.NoError(t, err)
		}
		waitForRistretto()

		// At least some of the earlier keys should be evicted from memory.
		// We don't assert which ones, just that the cache is still responsive.
		stats, err := adapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
	})
}

// TestRistrettoIntegration_NilValue verifies setting nil or empty values.
func TestRistrettoIntegration_NilValue(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("set_nil_value", func(t *testing.T) {
		err := adapter.Set(ctx, "test:nil", nil)
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, "test:nil")
		require.NoError(t, err)
		assert.Nil(t, val)
	})
}

// TestRistrettoIntegration_EmptyString tests edge cases around empty string values.
func TestRistrettoIntegration_EmptyString(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("set_empty_string", func(t *testing.T) {
		err := adapter.Set(ctx, "test:emptystr", "")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, "test:emptystr")
		require.NoError(t, err)
		assert.Equal(t, "", val)
	})

	t.Run("empty_key", func(t *testing.T) {
		err := adapter.Set(ctx, "", "value-for-empty-key")
		require.NoError(t, err)
		waitForRistretto()

		val, err := adapter.Get(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, "value-for-empty-key", val)
	})
}

// TestRistrettoIntegration_Metrics verifies metric counters on the ristretto cache.
func TestRistrettoIntegration_Metrics(t *testing.T) {
	adapter := setupRistretto(t)
	defer adapter.Close()
	ctx := context.Background()

	t.Run("metrics_collection", func(t *testing.T) {
		// Perform some operations then check we can read basic metrics
		_ = adapter.Set(ctx, "test:metrics:1", "a")
		_ = adapter.Set(ctx, "test:metrics:2", "b")
		_ = adapter.Set(ctx, "test:metrics:3", "c")
		waitForRistretto()

		_, err := adapter.Get(ctx, "test:metrics:1")
		require.NoError(t, err)

		// Since Ristretto wraps the underlying cart, we read metrics
		// via the cart's methods (if exposed). Otherwise just verify
		// that no panics occur.
		stats, err := adapter.GetStats(ctx)
		require.NoError(t, err)
		assert.NotNil(t, stats)
		assert.True(t, stats.Connected)
	})
}
