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

const (
	testSmallConcurrency = 10
	testLargeConcurrency = 100
	testMedConcurrency   = 20
	testLoaderDelay10ms  = 10 * time.Millisecond
	testLoaderDelay30ms  = 30 * time.Millisecond
	testLoaderDelay50ms  = 50 * time.Millisecond
	testLoaderDelay800ms = 800 * time.Millisecond
	testLockTTL200ms     = 200 * time.Millisecond
	testExpiry100ms      = 100 * time.Millisecond
	testFastForward150ms = 150 * time.Millisecond
	testFastForward2ms   = 2 * time.Millisecond
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

func newTestCart(t *testing.T) (*redis.RedisCart, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)

	client := goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})

	return redis.NewRedis(client), mr
}

func newTestCartWithConfig(t *testing.T, opts ...redis.RedisCartOption) (*redis.RedisCart, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)

	client := goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})

	return redis.NewRedis(client, opts...), mr
}

func TestRedisCart_SetGet(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	value := map[string]interface{}{
		"name":   "Test",
		"count":  42,
		"active": true,
	}

	err := c.Set(ctx, "test:key", value, 1*time.Hour)
	require.NoError(t, err)

	result, err := c.Get(ctx, "test:key")
	require.NoError(t, err)

	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Test", resultMap["name"])
	assert.Equal(t, float64(42), resultMap["count"])
	assert.Equal(t, true, resultMap["active"])
}

func TestRedisCart_GetMiss(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	_, err := c.Get(context.Background(), "non:existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value not found")
}

func TestRedisCart_Delete(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()

	err := c.Set(ctx, "test:del", "value", 1*time.Hour)
	require.NoError(t, err)

	exists, err := c.Exists(ctx, "test:del")
	require.NoError(t, err)
	assert.True(t, exists)

	err = c.Delete(ctx, "test:del")
	require.NoError(t, err)

	exists, err = c.Exists(ctx, "test:del")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRedisCart_SetGetMulti(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	items := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	err := c.SetMulti(ctx, items, 1*time.Hour)
	require.NoError(t, err)

	results, err := c.GetMulti(ctx, []string{"key1", "key2", "key3"})
	require.NoError(t, err)
	assert.Len(t, results, 3)
	assert.Equal(t, "value1", results["key1"])
	assert.Equal(t, "value2", results["key2"])
	assert.Equal(t, "value3", results["key3"])
}

func TestRedisCart_DeleteMulti(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	items := map[string]interface{}{
		"del1": "v1",
		"del2": "v2",
		"del3": "v3",
	}

	err := c.SetMulti(ctx, items, 1*time.Hour)
	require.NoError(t, err)

	err = c.DeleteMulti(ctx, []string{"del1", "del2"})
	require.NoError(t, err)

	exists, err := c.Exists(ctx, "del1")
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = c.Exists(ctx, "del3")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestRedisCart_AcquireReleaseLock(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:lock:key"

	lockValue1, acquired, err := c.AcquireLock(ctx, key, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, lockValue1)

	_, acquired, err = c.AcquireLock(ctx, key, 10*time.Second)
	require.NoError(t, err)
	assert.False(t, acquired, "should not acquire when already held")

	err = c.ReleaseLock(ctx, key, lockValue1)
	require.NoError(t, err)

	lockValue2, acquired, err := c.AcquireLock(ctx, key, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired, "should acquire after release")
	assert.NotEqual(t, lockValue1, lockValue2, "lock values should be unique")
}

func TestRedisCart_LockExpiration(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:lock:expiry"

	lockValue, acquired, err := c.AcquireLock(ctx, key, 1*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, acquired)

	mr.FastForward(testFastForward2ms)

	_, acquired, err = c.AcquireLock(ctx, key, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired, "should acquire after TTL expires")

	err = c.ReleaseLock(ctx, key, lockValue)
	require.NoError(t, err)
}

func TestRedisCart_ExtendLock(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:lock:extend"

	lockValue, acquired, err := c.AcquireLock(ctx, key, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	extended, err := c.ExtendLock(ctx, key, lockValue, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, extended, "should extend with correct value")

	extended, err = c.ExtendLock(ctx, key, "wrong-value", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, extended, "should not extend with wrong value")
}

func TestRedisCart_GetOrSetWithLock_Single(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:single"
	loaderCalls := int32(0)

	loader := func(_ context.Context) (interface{}, error) {
		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(testLoaderDelay10ms)

		return "loaded-value", nil
	}

	result, err := c.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "loaded-value", result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls))

	result, err = c.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "loaded-value", result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "loader should not be called again (cached)")
}

func TestRedisCart_GetOrSetWithLock_Concurrent(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:concurrent"
	loaderCalls := int32(0)

	loader := func(ctx context.Context) (interface{}, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(testLoaderDelay50ms)

		return "loaded-value", nil
	}

	results := make([]interface{}, testSmallConcurrency)
	errs := make([]error, testSmallConcurrency)

	var wg sync.WaitGroup

	for i := 0; i < testSmallConcurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = c.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
		}(i)
	}

	wg.Wait()

	for i := 0; i < testSmallConcurrency; i++ {
		require.NoError(t, errs[i], "request %d should not error", i)
		assert.Equal(t, "loaded-value", results[i])
	}

	calls := atomic.LoadInt32(&loaderCalls)
	assert.LessOrEqual(t, calls, int32(2), "stampede prevention: loader called %d times", calls)
}

func TestRedisCart_GetOrSetWithLock_LoaderError(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	key := "test:loadererr"

	loader := func(_ context.Context) (interface{}, error) {
		return nil, fmt.Errorf("database error")
	}

	_, err := c.GetOrSetWithLock(ctx, key, loader, 1*time.Hour, 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loader failed")

	exists, err := c.Exists(ctx, key)
	require.NoError(t, err)
	assert.False(t, exists, "failed load should not be cached")
}

func TestRedisCart_HealthCheck(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	err := c.HealthCheck(context.Background())
	assert.NoError(t, err)
}

func TestRedisCart_HealthCheck_Unhealthy(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer c.Close() //nolint:errcheck

	mr.Close()

	err := c.HealthCheck(context.Background())
	assert.Error(t, err)
}

func TestRedisCart_Expiration(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()

	err := c.Set(ctx, "test:expiry", "expires-soon", testExpiry100ms)
	require.NoError(t, err)

	result, err := c.Get(ctx, "test:expiry")
	require.NoError(t, err)
	assert.Equal(t, "expires-soon", result)

	mr.FastForward(testFastForward150ms)

	_, err = c.Get(ctx, "test:expiry")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value not found")
}

func TestRedisCart_CustomLockConfig(t *testing.T) {
	t.Parallel()

	c, mr := newTestCartWithConfig(t,
		redis.WithLockTTL(5*time.Second),
		redis.WithLockMaxWait(1*time.Second),
		redis.WithLockBackoff(25*time.Millisecond, 250*time.Millisecond),
		redis.WithLockMultiplier(3.0),
	)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	loaderCalls := int32(0)

	loader := func(_ context.Context) (interface{}, error) {
		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(testLoaderDelay10ms)

		return "test-value", nil
	}

	result, err := c.GetOrSetWithLock(ctx, "custom:key", loader, 1*time.Hour, 0)
	require.NoError(t, err)
	assert.Equal(t, "test-value", result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls))

	result, err = c.GetOrSetWithLock(ctx, "custom:key", loader, 1*time.Hour, 0)
	require.NoError(t, err)
	assert.Equal(t, "test-value", result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "should be cached")
}

func TestRedisCart_HighConcurrency(t *testing.T) {
	t.Parallel()

	c, mr := newTestCartWithConfig(t,
		redis.WithLockTTL(2*time.Second),
		redis.WithLockMaxWait(5*time.Second),
	)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	loaderCalls := int32(0)

	loader := func(ctx context.Context) (interface{}, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(testLoaderDelay30ms)

		return "hot-data", nil
	}

	results := make([]interface{}, testLargeConcurrency)
	errs := make([]error, testLargeConcurrency)

	runConcurrentWithStart(testLargeConcurrency, func(idx int) {
		results[idx], errs[idx] = c.GetOrSetWithLock(ctx, "prod:concurrency", loader, 1*time.Hour, 2*time.Second)
	})

	for i := 0; i < testLargeConcurrency; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, "hot-data", results[i])
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "loader should be called exactly once")
}

func TestRedisCart_LoaderFailure(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	loaderCalls := int32(0)

	loader := func(_ context.Context) (interface{}, error) {
		atomic.AddInt32(&loaderCalls, 1)

		return nil, fmt.Errorf("loader failed")
	}

	_, err := c.GetOrSetWithLock(ctx, "prod:failure", loader, 1*time.Hour, 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loader failed")

	exists, err := c.Exists(ctx, "prod:failure")
	require.NoError(t, err)
	assert.False(t, exists, "failed load should never populate cache")
	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls))
}

func TestRedisCart_LockHeartbeat(t *testing.T) {
	t.Parallel()

	c, mr := newTestCartWithConfig(t,
		redis.WithLockTTL(testLockTTL200ms),
		redis.WithLockMaxWait(5*time.Second),
	)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

	ctx := context.Background()
	loaderCalls := int32(0)

	loader := func(ctx context.Context) (interface{}, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		atomic.AddInt32(&loaderCalls, 1)
		time.Sleep(testLoaderDelay800ms)

		return "long-running", nil
	}

	results := make([]interface{}, testMedConcurrency)
	errs := make([]error, testMedConcurrency)

	runConcurrentWithStart(testMedConcurrency, func(idx int) {
		results[idx], errs[idx] = c.GetOrSetWithLock(ctx, "prod:heartbeat", loader, 1*time.Hour, testLockTTL200ms)
	})

	for i := 0; i < testMedConcurrency; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, "long-running", results[i])
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&loaderCalls), "heartbeat should prevent duplicate loaders")
}

func TestRedisCart_ConnectionLoss(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer c.Close() //nolint:errcheck

	ctx := context.Background()

	err := c.Set(ctx, "prod:conn", "value", 1*time.Hour)
	require.NoError(t, err)

	mr.Close()

	_, err = c.Get(ctx, "prod:conn")
	assert.Error(t, err, "get should fail when redis is unavailable")

	err = c.Set(ctx, "prod:conn", "value2", 1*time.Hour)
	assert.Error(t, err, "set should fail when redis is unavailable")
}

func TestRedisCart_ContextCancellation(t *testing.T) {
	t.Parallel()

	c, mr := newTestCart(t)
	defer mr.Close()
	defer c.Close() //nolint:errcheck

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
		_, err := c.GetOrSetWithLock(ctx, "prod:cancel", loader, 1*time.Hour, 5*time.Second)
		resultCh <- err
	}()

	<-loaderStarted
	cancel()

	err := <-resultCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), context.Canceled.Error())

	exists, err := c.Exists(context.Background(), "prod:cancel")
	require.NoError(t, err)
	assert.False(t, exists, "cancelled loader should not populate cache")
}
