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

// TestDragonflyIntegration_Set tests the Set operation.
func TestDragonflyIntegration_Set(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
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
