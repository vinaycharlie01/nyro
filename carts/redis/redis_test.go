package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinaycharlie01/nyro/carts/redis"
)

func runConcurrentWithStart(concurrency int, fn func(idx int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			fn(idx)
		}(i)
	}

	close(start)
	wg.Wait()
}

func setupTestRedis(t *testing.T) (*redis.RedisStore, *miniredis.Miniredis) {
	t.Helper()

	mockRedis := miniredis.RunT(t)

	client := goredis.NewClient(&goredis.Options{
		Addr: mockRedis.Addr(),
	})

	store := redis.NewRedis(client)
	return store, mockRedis
}

func setupTestRedisWithConfig(t *testing.T, opts ...redis.RedisStoreOption) (*redis.RedisStore, *miniredis.Miniredis) {
	t.Helper()

	mockRedis := miniredis.RunT(t)

	client := goredis.NewClient(&goredis.Options{
		Addr: mockRedis.Addr(),
	})

	store := redis.NewRedis(client, opts...)
	return store, mockRedis
}

func TestRedisStore_BasicOperations(t *testing.T) {
	store, mockRedis := setupTestRedis(t)
	defer mockRedis.Close()
	defer store.Close()

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		key := "test:key"
		value := map[string]interface{}{
			"name":   "Test",
			"count":  42,
			"active": true,
		}

		err := store.Set(ctx, key, value, 1*time.Hour)
		require.NoError(t, err)

		result, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.NotNil(t, result)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "Test", resultMap["name"])
		assert.Equal(t, float64(42), resultMap["count"])
		assert.Equal(t, true, resultMap["active"])
	})

	t.Run("Get Non-Existent Key", func(t *testing.T) {
		_, err := store.Get(ctx, "non:existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value not found")
	})

	t.Run("Delete", func(t *testing.T) {
		key := "test:delete"

		err := store.Set(ctx, key, "value", 1*time.Hour)
		require.NoError(t, err)

		exists, err := store.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		err = store.Delete(ctx, key)
		require.NoError(t, err)

		exists, err = store.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("SetMulti and GetMulti", func(t *testing.T) {
		items := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		err := store.SetMulti(ctx, items, 1*time.Hour)
		require.NoError(t, err)

		keys := []string{"key1", "key2", "key3"}
		results, err := store.GetMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, results, 3)
		assert.Equal(t, "value1", results["key1"])
		assert.Equal(t, "value2", results["key2"])
		assert.Equal(t, "value3", results["key3"])
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		items := map[string]interface{}{
			"del1": "value1",
			"del2": "value2",
			"del3": "value3",
		}
		err := store.SetMulti(ctx, items, 1*time.Hour)
		require.NoError(t, err)

		err = store.DeleteMulti(ctx, []string{"del1", "del2"})
		require.NoError(t, err)

		exists, err := store.Exists(ctx, "del1")
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = store.Exists(ctx, "del3")
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestRedisStore_DistributedLocking(t *testing.T) {
	store, mockRedis := setupTestRedis(t)
	defer mockRedis.Close()
	defer store.Close()

	ctx := context.Background()

	t.Run("Acquire and Release Lock", func(t *testing.T) {
		key := "test:lock:key"

		lockValue1, acquired, err := store.AcquireLock(ctx, key, 10*time.Second)
		require.NoError(t, err)
		assert.True(t, acquired, "should acquire lock on first attempt")
		assert.NotEmpty(t, lockValue1, "lock value should not be empty")

		_, acquired, err = store.AcquireLock(ctx, key, 10*time.Second)
		require.NoError(t, err)
		assert.False(t, acquired, "should not acquire lock when already held")

		err = store.ReleaseLock(ctx, key, lockValue1)
		require.NoError(t, err)

		lockValue2, acquired, err := store.AcquireLock(ctx, key, 10*time.Second)
		require.NoError(t, err)
		assert.True(t, acquired, "should acquire lock after release")
		assert.NotEmpty(t, lockValue2, "lock value should not be empty")
		assert.NotEqual(t, lockValue1, lockValue2, "lock values should be unique")
	})

	t.Run("Lock Expiration", func(t *testing.T) {
		key := "test:lock:expiry"

		lockValue, acquired, err := store.AcquireLock(ctx, key, 1*time.Millisecond)
		require.NoError(t, err)
		assert.True(t, acquired)

		mockRedis.FastForward(2 * time.Millisecond)

		_, acquired, err = store.AcquireLock(ctx, key, 10*time.Second)
		require.NoError(t, err)
		assert.True(t, acquired, "should acquire lock after TTL expires")

		err = store.ReleaseLock(ctx, key, lockValue)
		require.NoError(t, err)
	})

	t.Run("Extend Lock", func(t *testing.T) {
		key := "test:lock:extend"

		lockValue, acquired, err := store.AcquireLock(ctx, key, 5*time.Second)
		require.NoError(t, err)
		assert.True(t, acquired)

		extended, err := store.ExtendLock(ctx, key, lockValue, 10*time.Second)
		require.NoError(t, err)
		assert.True(t, extended, "should extend lock with correct value")

		wrongValue := "wrong-lock-value"
		extended, err = store.ExtendLock(ctx, key, wrongValue, 10*time.Second)
		require.NoError(t, err)
		assert.False(t, extended, "should not extend lock with wrong value")
	})
}

func TestRedisStore_CacheStampedePrevention(t *testing.T) {
	store, mockRedis := setupTestRedis(t)
	defer mockRedis.Close()
	defer store.Close()

	ctx := context.Background()

	t.Run("GetOrSetWithLock - Single Request", func(t *testing.T) {
		key := "test:stampede:single"
		expectedValue := "loaded-value"
		loaderCalls := int32(0)

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(10 * time.Millisecond)
			return expectedValue, nil
		}

		result, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, result)
		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "loader should be called once")

		result, err = store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, result)
		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "loader should still be called only once (cached)")
	})

	t.Run("GetOrSetWithLock - Concurrent Requests (Cache Stampede)", func(t *testing.T) {
		key := "test:stampede:concurrent"
		expectedValue := "loaded-value"
		loaderCalls := int32(0)

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(50 * time.Millisecond)
			return expectedValue, nil
		}

		concurrency := 10
		var wg sync.WaitGroup
		results := make([]interface{}, concurrency)
		errors := make([]error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				result, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
				results[index] = result
				errors[index] = err
			}(i)
		}

		wg.Wait()

		for i := 0; i < concurrency; i++ {
			require.NoError(t, errors[i], "request %d should not error", i)
			assert.Equal(t, expectedValue, results[i], "request %d should get correct value", i)
		}

		calls := atomic.LoadInt32(&loaderCalls)
		assert.LessOrEqual(t, calls, int32(2),
			"loader should be called at most 2 times (ideally 1, allowing 1 retry) but was called %d times - STAMPEDE NOT PREVENTED!", calls)
	})

	t.Run("GetOrSetWithLock - Loader Error", func(t *testing.T) {
		key := "test:stampede:error"
		expectedErr := fmt.Errorf("database error")

		loader := func(ctx context.Context) (interface{}, error) {
			return nil, expectedErr
		}

		_, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "loader failed")

		exists, err := store.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists, "failed load should not be cached")
	})
}

func TestRedisStore_HealthCheck(t *testing.T) {
	store, mockRedis := setupTestRedis(t)
	defer mockRedis.Close()
	defer store.Close()

	ctx := context.Background()

	t.Run("Healthy Connection", func(t *testing.T) {
		err := store.HealthCheck(ctx)
		assert.NoError(t, err)
	})

	t.Run("Unhealthy Connection", func(t *testing.T) {
		mockRedis.Close()

		err := store.HealthCheck(ctx)
		assert.Error(t, err)
	})
}

func TestRedisStore_Expiration(t *testing.T) {
	store, mockRedis := setupTestRedis(t)
	defer mockRedis.Close()
	defer store.Close()

	ctx := context.Background()

	t.Run("Value Expires After TTL", func(t *testing.T) {
		key := "test:expiry"
		value := "expires-soon"

		err := store.Set(ctx, key, value, 100*time.Millisecond)
		require.NoError(t, err)

		result, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, result)

		mockRedis.FastForward(150 * time.Millisecond)

		_, err = store.Get(ctx, key)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value not found")
	})
}

func TestRedisStore_CustomConfiguration(t *testing.T) {
	t.Run("Custom Lock Configuration", func(t *testing.T) {
		store, mockRedis := setupTestRedisWithConfig(t,
			redis.WithLockTTL(5*time.Second),
			redis.WithLockMaxWait(1*time.Second),
			redis.WithLockBackoff(25*time.Millisecond, 250*time.Millisecond),
			redis.WithLockMultiplier(3.0),
		)
		defer mockRedis.Close()
		defer store.Close()

		ctx := context.Background()
		key := "test:custom:config"
		expectedValue := "test-value"
		loaderCalls := int32(0)

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(10 * time.Millisecond)
			return expectedValue, nil
		}

		result, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 0)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, result)
		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls))

		result, err = store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 0)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, result)
		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "should still be 1 (cached)")
	})

	t.Run("Default Configuration", func(t *testing.T) {
		store, mockRedis := setupTestRedis(t)
		defer mockRedis.Close()
		defer store.Close()

		ctx := context.Background()
		err := store.Set(ctx, "test:key", "test:value", 1*time.Minute)
		require.NoError(t, err)

		result, err := store.Get(ctx, "test:key")
		require.NoError(t, err)
		assert.Equal(t, "test:value", result)
	})
}

func TestRedisStore_ProductionScenarios(t *testing.T) {
	t.Run("High Concurrency 100 Requests", func(t *testing.T) {
		store, mockRedis := setupTestRedisWithConfig(t,
			redis.WithLockTTL(2*time.Second),
			redis.WithLockMaxWait(5*time.Second),
		)
		defer mockRedis.Close()
		defer store.Close()

		ctx := context.Background()
		key := "prod:high:concurrency"
		expected := "hot-data"
		loaderCalls := int32(0)
		concurrency := 100

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(30 * time.Millisecond)
			return expected, nil
		}

		results := make([]interface{}, concurrency)
		errors := make([]error, concurrency)

		runConcurrentWithStart(concurrency, func(idx int) {
			result, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 2*time.Second)
			results[idx] = result
			errors[idx] = err
		})

		for i := 0; i < concurrency; i++ {
			require.NoError(t, errors[i], "request %d should not error", i)
			assert.Equal(t, expected, results[i], "request %d should get expected value", i)
		}

		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "loader should be called exactly once")
	})

	t.Run("Loader Failure Should Not Cache", func(t *testing.T) {
		store, mockRedis := setupTestRedis(t)
		defer mockRedis.Close()
		defer store.Close()

		ctx := context.Background()
		key := "prod:loader:failure"
		expectedErr := fmt.Errorf("loader failed")
		loaderCalls := int32(0)

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			return nil, expectedErr
		}

		_, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "loader failed")

		exists, err := store.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists, "failed load should never populate cache")
		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls))
	})

	t.Run("Long Loader With Short LockTTL", func(t *testing.T) {
		store, mockRedis := setupTestRedisWithConfig(t,
			redis.WithLockTTL(200*time.Millisecond),
			redis.WithLockMaxWait(5*time.Second),
		)
		defer mockRedis.Close()
		defer store.Close()

		ctx := context.Background()
		key := "prod:lock:heartbeat"
		expected := "long-running"
		loaderCalls := int32(0)

		loader := func(ctx context.Context) (interface{}, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(800 * time.Millisecond)
			return expected, nil
		}

		concurrency := 20
		results := make([]interface{}, concurrency)
		errors := make([]error, concurrency)

		runConcurrentWithStart(concurrency, func(idx int) {
			result, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 200*time.Millisecond)
			results[idx] = result
			errors[idx] = err
		})

		for i := 0; i < concurrency; i++ {
			require.NoError(t, errors[i], "request %d should not error", i)
			assert.Equal(t, expected, results[i])
		}

		assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "heartbeat should prevent duplicate loaders")
	})

	t.Run("Redis Connection Loss", func(t *testing.T) {
		store, mockRedis := setupTestRedis(t)
		defer store.Close()

		ctx := context.Background()
		err := store.Set(ctx, "prod:conn:key", "value", 1*time.Hour)
		require.NoError(t, err)

		mockRedis.Close()

		_, err = store.Get(ctx, "prod:conn:key")
		assert.Error(t, err, "get should fail when redis is unavailable")

		err = store.Set(ctx, "prod:conn:key", "value2", 1*time.Hour)
		assert.Error(t, err, "set should fail when redis is unavailable")
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		store, mockRedis := setupTestRedis(t)
		defer mockRedis.Close()
		defer store.Close()

		key := "prod:ctx:cancel"
		ctx, cancel := context.WithCancel(context.Background())

		loaderStarted := make(chan struct{})
		loader := func(loaderCtx context.Context) (interface{}, error) {
			close(loaderStarted)
			select {
			case <-loaderCtx.Done():
				return nil, loaderCtx.Err()
			case <-time.After(2 * time.Second):
				return "unexpected", nil
			}
		}

		resultCh := make(chan error, 1)
		go func() {
			_, err := store.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
			resultCh <- err
		}()

		<-loaderStarted
		cancel()

		err := <-resultCh
		require.Error(t, err)
		assert.Contains(t, err.Error(), context.Canceled.Error())

		exists, err := store.Exists(context.Background(), key)
		require.NoError(t, err)
		assert.False(t, exists, "cancelled loader should not populate cache")
	})
}
