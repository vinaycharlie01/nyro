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

// TestRedisIntegration_Get tests the Get operation
func TestRedisIntegration_Get(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("get_existing_key", func(t *testing.T) {
		key := "test:get:existing"
		expectedValue := "test-value"

		// Set value first
		err := cacheAdapter.Set(ctx, key, expectedValue)
		require.NoError(t, err)

		// Get value
		val, err := cacheAdapter.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, val)
	})

	t.Run("get_non_existing_key", func(t *testing.T) {
		key := "test:get:nonexistent"

		// Get non-existent key
		val, err := cacheAdapter.Get(ctx, key)
		assert.Error(t, err)
		assert.Nil(t, val)
	})

	t.Run("get_with_different_key_types", func(t *testing.T) {
		tests := []struct {
			name  string
			key   any
			value string
		}{
			{"string_key", "test:string", "value1"},
			{"int_key", 12345, "value2"},
			{"struct_key", struct{ ID int }{ID: 1}, "value3"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := cacheAdapter.Set(ctx, tt.key, tt.value)
				require.NoError(t, err)

				val, err := cacheAdapter.Get(ctx, tt.key)
				require.NoError(t, err)
				assert.Equal(t, tt.value, val)
			})
		}
	})
}
