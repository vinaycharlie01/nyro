//go:build integration
// +build integration

package redis_integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	testcontainers "github.com/vinaycharlie01/nyro/test/test-containers"
)

// TestRedisIntegration_HighConcurrency tests high concurrency scenarios
func TestRedisIntegration_HighConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		concurrent  int
		loaderDelay time.Duration
		wantValue   string
		wantCalls   int32
	}{
		{
			name:        "100_concurrent_requests",
			concurrent:  100,
			loaderDelay: 200 * time.Millisecond,
			wantValue:   "loaded-value",
			wantCalls:   1,
		},
		{
			name:        "50_concurrent_fast_loader",
			concurrent:  50,
			loaderDelay: 50 * time.Millisecond,
			wantValue:   "fast-value",
			wantCalls:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			redis := testcontainers.SetupRedisContainer(t)
			cacheAdapter := redis.GetCacheAdapter()
			ctx := context.Background()
			key := fmt.Sprintf("test:concurrency:%s", tt.name)
			calls := int32(0)

			loader := func(ctx context.Context) (interface{}, error) {
				atomic.AddInt32(&calls, 1)
				time.Sleep(tt.loaderDelay)
				return tt.wantValue, nil
			}

			// Execute
			results := make([]struct {
				val any
				err error
			}, tt.concurrent)

			runConcurrent(tt.concurrent, func(idx int) {
				val, err := cacheAdapter.GetOrSet(ctx, key, loader)
				results[idx].val = val
				results[idx].err = err
			})

			// Verify
			for i := 0; i < tt.concurrent; i++ {
				require.NoError(t, results[i].err)
				assert.Equal(t, tt.wantValue, results[i].val)
			}
			assert.Equal(t, tt.wantCalls, atomic.LoadInt32(&calls))
		})
	}
}

// TestRedisIntegration_CacheStampede tests cache stampede prevention
func TestRedisIntegration_CacheStampede(t *testing.T) {
	// Setup
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()
	key := "test:stampede"
	calls := int32(0)

	loader := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(500 * time.Millisecond)
		return fmt.Sprintf("data-%d", atomic.LoadInt32(&calls)), nil
	}

	tests := []struct {
		name       string
		wait       time.Duration
		concurrent int
		wantCalls  int32
	}{
		{name: "wave_1", wait: 0, concurrent: 50, wantCalls: 1},
		{name: "wave_2", wait: 1500 * time.Millisecond, concurrent: 50, wantCalls: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			if tt.wait > 0 {
				time.Sleep(tt.wait)
			}

			// Execute
			results := make([]error, tt.concurrent)
			runConcurrent(tt.concurrent, func(idx int) {
				_, err := cacheAdapter.GetOrSet(ctx, key, loader, cache.WithTTL(1*time.Second))
				results[idx] = err
			})

			// Verify
			for i := 0; i < tt.concurrent; i++ {
				assert.NoError(t, results[i])
			}
			assert.Equal(t, tt.wantCalls, atomic.LoadInt32(&calls))
		})
	}
}

// TestRedisIntegration_RealWorldScenario tests real-world usage patterns
func TestRedisIntegration_RealWorldScenario(t *testing.T) {
	// Setup
	redis := testcontainers.SetupRedisContainer(t)
	cacheAdapter := redis.GetCacheAdapter()
	ctx := context.Background()

	userIDs := []string{"user-1", "user-2", "user-3"}
	resources := []string{"profile", "settings"}
	callsPerKey := sync.Map{}

	type job struct {
		key string
	}
	var jobs []job

	for _, user := range userIDs {
		for _, res := range resources {
			key := fmt.Sprintf("user:%s:%s", user, res)
			callsPerKey.Store(key, new(int32))
			for i := 0; i < 10; i++ {
				jobs = append(jobs, job{key: key})
			}
		}
	}

	// Execute
	results := make([]error, len(jobs))
	runConcurrent(len(jobs), func(idx int) {
		key := jobs[idx].key
		loader := func(ctx context.Context) (interface{}, error) {
			counter, _ := callsPerKey.Load(key)
			atomic.AddInt32(counter.(*int32), 1)
			time.Sleep(100 * time.Millisecond)
			return fmt.Sprintf("data-%s", key), nil
		}
		_, err := cacheAdapter.GetOrSet(ctx, key, loader)
		results[idx] = err
	})

	// Verify
	for i := range results {
		assert.NoError(t, results[i])
	}

	callsPerKey.Range(func(key, value interface{}) bool {
		calls := atomic.LoadInt32(value.(*int32))
		assert.Equal(t, int32(1), calls, "key %s should only load once", key)
		return true
	})
}
