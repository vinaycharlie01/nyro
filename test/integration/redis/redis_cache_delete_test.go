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

// TestRedisIntegration_Delete tests the Delete operation
func TestRedisIntegration_Delete(t *testing.T) {
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	t.Run("delete_existing_key", func(t *testing.T) {
		key := "test:delete:existing"
		value := "to-be-deleted"

		// Set value
		err := cacheAdapter.Set(ctx, key, value)
		require.NoError(t, err)

		// Delete value
		err = cacheAdapter.Delete(ctx, key)
		require.NoError(t, err)

		// Verify deletion
		_, err = cacheAdapter.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("delete_non_existing_key", func(t *testing.T) {
		key := "test:delete:nonexistent"

		// Delete non-existent key (should not error)
		err := cacheAdapter.Delete(ctx, key)
		assert.NoError(t, err)
	})
}
