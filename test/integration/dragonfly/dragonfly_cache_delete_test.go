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

// TestDragonflyIntegration_Delete tests the Delete operation.
func TestDragonflyIntegration_Delete(t *testing.T) {
	dragonfly := testcontainers.SetupDragonflyContainer(t)
	cacheAdapter := dragonfly.GetCacheAdapter()
	ctx := context.Background()

	t.Run("delete_existing_key", func(t *testing.T) {
		key := "test:delete:existing"
		value := "to-be-deleted"

		err := cacheAdapter.Set(ctx, key, value)
		require.NoError(t, err)

		err = cacheAdapter.Delete(ctx, key)
		require.NoError(t, err)

		_, err = cacheAdapter.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("delete_non_existing_key", func(t *testing.T) {
		key := "test:delete:nonexistent"

		err := cacheAdapter.Delete(ctx, key)
		assert.NoError(t, err)
	})
}
