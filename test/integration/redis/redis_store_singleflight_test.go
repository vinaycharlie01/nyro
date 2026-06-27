//go:build integration
// +build integration

package redis_integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storeRedis "github.com/vinaycharlie01/nyro/carts/redis"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

func TestRedisStore_GetOrSetWithLock_SingleflightDedup(t *testing.T) {
	redisContainer := testcontainers.SetupRedisContainer(t)
	store := storeRedis.NewRedis(redisContainer.GetClient())

	ctx := context.Background()
	key := "test:singleflight:dedup"
	loaderDelay := 200 * time.Millisecond

	var calls int32
	loader := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(loaderDelay)
		return "singleflight-value", nil
	}

	const concurrent = 40
	results := make([]struct {
		val any
		err error
	}, concurrent)

	runConcurrent(concurrent, func(idx int) {
		v, err := store.GetOrSetWithLock(
			ctx,
			key,
			loader,
			5*time.Second,
			3*time.Second,
		)
		results[idx].val = v
		results[idx].err = err
	})

	for i := 0; i < concurrent; i++ {
		require.NoError(t, results[i].err)
		assert.Equal(t, "singleflight-value", results[i].val)
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestRedisStore_GetOrSetWithLock_WaitsForCacheWhenLockHeld(t *testing.T) {
	redisContainer := testcontainers.SetupRedisContainer(t)
	store := storeRedis.NewRedis(redisContainer.GetClient())

	ctx := context.Background()
	key := "test:wait-for-cache:lock-held"

	// Pre-hold the distributed lock so GetOrSetWithLock cannot acquire it.
	lockValue, acquired, err := store.AcquireLock(ctx, key, 3*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	defer func() {
		releaseErr := store.ReleaseLock(ctx, key, lockValue)
		require.NoError(t, releaseErr)
	}()

	var loaderCalls int32
	loader := func(ctx context.Context) (any, error) {
		atomic.AddInt32(&loaderCalls, 1)
		return "should-not-run", nil
	}

	go func() {
		// Simulate lock owner populating cache while others wait.
		time.Sleep(150 * time.Millisecond)
		_ = store.Set(context.Background(), key, "value-from-owner", 10*time.Second)
	}()

	val, err := store.GetOrSetWithLock(ctx, key, loader, 10*time.Second, 3*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "value-from-owner", val)
	assert.Equal(t, int32(0), atomic.LoadInt32(&loaderCalls))
}
