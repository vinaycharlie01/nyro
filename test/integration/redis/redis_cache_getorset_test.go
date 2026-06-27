//go:build integration
// +build integration

package redis_integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestRedisIntegration_GetOrSet tests the GetOrSet operation
func TestRedisIntegration_GetOrSet(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_or_set_cache_miss", func(t *testing.T) {
		key := "test:getorset:miss"
		expectedValue := "loaded-value"
		loaderCalled := false

		loader := func(ctx context.Context) (any, error) {
			loaderCalled = true
			return expectedValue, nil
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, val)
		assert.True(t, loaderCalled, "loader should be called on cache miss")
	})

	t.Run("get_or_set_cache_hit", func(t *testing.T) {
		key := "test:getorset:hit"
		cachedValue := "cached-value"
		loaderCalled := false

		// Pre-populate cache
		err := cacheAdapter.Set(ctx, key, cachedValue)
		require.NoError(t, err)

		loader := func(ctx context.Context) (any, error) {
			loaderCalled = true
			return "should-not-be-called", nil
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader)
		require.NoError(t, err)
		assert.Equal(t, cachedValue, val)
		assert.False(t, loaderCalled, "loader should not be called on cache hit")
	})

	t.Run("get_or_set_loader_error", func(t *testing.T) {
		key := "test:getorset:error"
		expectedError := errors.New("loader failed")

		loader := func(ctx context.Context) (any, error) {
			return nil, expectedError
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader)
		assert.Error(t, err)
		assert.Nil(t, val)
		assert.Contains(t, err.Error(), "loader failed")
	})

	t.Run("get_or_set_with_ttl", func(t *testing.T) {
		key := "test:getorset:ttl"
		value := "ttl-value"

		loader := func(ctx context.Context) (any, error) {
			return value, nil
		}

		val, err := cacheAdapter.GetOrSet(ctx, key, loader, cache.WithTTL(1*time.Second))
		require.NoError(t, err)
		assert.Equal(t, value, val)

		// Value should exist
		exists, err := cacheAdapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Wait for expiration
		time.Sleep(1500 * time.Millisecond)

		// Value should be expired
		exists, err = cacheAdapter.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}
