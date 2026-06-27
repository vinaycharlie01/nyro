package cache_adapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	store "github.com/vinaycharlie01/nyro/stores"
)

func setupTestRedis(t *testing.T) (*RedisAdapter, *miniredis.Miniredis) {
	t.Helper()
	mockRedis := miniredis.RunT(t)

	config := RedisConfig{
		Addr:       mockRedis.Addr(),
		DefaultTTL: 1 * time.Hour,
	}

	adapter, err := NewRedisAdapter(config)
	require.NoError(t, err)

	return adapter, mockRedis
}

func TestRedisAdapter_BasicOperations(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*RedisAdapter, *miniredis.Miniredis)
		verify func(*testing.T, *RedisAdapter, *miniredis.Miniredis)
	}{
		{
			name: "set_and_get",
			setup: func(adapter *RedisAdapter, _ *miniredis.Miniredis) {
				err := adapter.Set(context.Background(), "key1", "value1")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, adapter *RedisAdapter, _ *miniredis.Miniredis) {
				val, err := adapter.Get(context.Background(), "key1")
				require.NoError(t, err)
				assert.Equal(t, "value1", val)
			},
		},
		{
			name:  "get_not_found",
			setup: func(*RedisAdapter, *miniredis.Miniredis) {},
			verify: func(t *testing.T, adapter *RedisAdapter, _ *miniredis.Miniredis) {
				_, err := adapter.Get(context.Background(), "nonexistent")
				assert.Error(t, err)

				var notFoundErr *store.NotFound
				assert.True(t, errors.As(err, &notFoundErr))
			},
		},
		{
			name: "delete",
			setup: func(adapter *RedisAdapter, _ *miniredis.Miniredis) {
				require.NoError(t, adapter.Set(context.Background(), "key2", "value2"))
			},
			verify: func(t *testing.T, adapter *RedisAdapter, _ *miniredis.Miniredis) {
				err := adapter.Delete(context.Background(), "key2")
				require.NoError(t, err)

				exists, err := adapter.Exists(context.Background(), "key2")
				require.NoError(t, err)
				assert.False(t, exists)
			},
		},
		{
			name: "exists",
			setup: func(adapter *RedisAdapter, _ *miniredis.Miniredis) {
				require.NoError(t, adapter.Set(context.Background(), "key3", "value3"))
			},
			verify: func(t *testing.T, adapter *RedisAdapter, _ *miniredis.Miniredis) {
				exists, err := adapter.Exists(context.Background(), "key3")
				require.NoError(t, err)
				assert.True(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, mockRedis := setupTestRedis(t)
			defer adapter.Close()
			defer mockRedis.Close()

			tt.setup(adapter, mockRedis)
			tt.verify(t, adapter, mockRedis)
		})
	}
}

func TestRedisAdapter_MultiOperations(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*RedisAdapter)
		verify func(*testing.T, *RedisAdapter)
	}{
		{
			name: "set_and_get_multi",
			setup: func(adapter *RedisAdapter) {
				items := map[any]any{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
				err := adapter.SetMulti(context.Background(), items)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, adapter *RedisAdapter) {
				keys := []any{"key1", "key2", "key3"}
				results, err := adapter.GetMulti(context.Background(), keys)
				require.NoError(t, err)
				assert.Len(t, results, 3)
				assert.Equal(t, "value1", results["key1"])
				assert.Equal(t, "value2", results["key2"])
				assert.Equal(t, "value3", results["key3"])
			},
		},
		{
			name: "delete_multi",
			setup: func(adapter *RedisAdapter) {
				ctx := context.Background()
				require.NoError(t, adapter.Set(ctx, "del1", "v1"))
				require.NoError(t, adapter.Set(ctx, "del2", "v2"))
			},
			verify: func(t *testing.T, adapter *RedisAdapter) {
				ctx := context.Background()
				err := adapter.DeleteMulti(ctx, []any{"del1", "del2"})
				require.NoError(t, err)

				exists, _ := adapter.Exists(ctx, "del1")
				assert.False(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, mockRedis := setupTestRedis(t)
			defer adapter.Close()
			defer mockRedis.Close()

			tt.setup(adapter)
			tt.verify(t, adapter)
		})
	}
}

func TestRedisAdapter_GetOrSet(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*miniredis.Miniredis)
		loaderValue   string
		loaderErr     error
		wantValue     string
		wantErr       bool
		wantLoaderRun bool
	}{
		{
			name:          "cache_hit",
			setup:         func(m *miniredis.Miniredis) { m.Set("cached-key", "cached-value") },
			loaderValue:   "loaded-value",
			wantValue:     "cached-value",
			wantLoaderRun: false,
		},
		{
			name:          "cache_miss",
			setup:         func(*miniredis.Miniredis) {},
			loaderValue:   "loaded-value",
			wantValue:     "loaded-value",
			wantLoaderRun: true,
		},
		{
			name:          "loader_error",
			setup:         func(*miniredis.Miniredis) {},
			loaderErr:     errors.New("load failed"),
			wantErr:       true,
			wantLoaderRun: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, mockRedis := setupTestRedis(t)
			defer adapter.Close()
			defer mockRedis.Close()

			// Setup
			tt.setup(mockRedis)

			loaderCalled := false
			loader := func(ctx context.Context) (any, error) {
				loaderCalled = true
				if tt.loaderErr != nil {
					return nil, tt.loaderErr
				}
				return tt.loaderValue, nil
			}

			// Execute
			key := "cached-key"
			if tt.name == "cache_miss" {
				key = "new-key"
			} else if tt.name == "loader_error" {
				key = "error-key"
			}

			result, err := adapter.GetOrSet(context.Background(), key, loader)

			// Verify
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantValue, result)
			}
			assert.Equal(t, tt.wantLoaderRun, loaderCalled)
		})
	}
}

func TestRedisAdapter_TTL(t *testing.T) {
	adapter, mockRedis := setupTestRedis(t)
	defer adapter.Close()
	defer mockRedis.Close()

	// Setup
	ctx := context.Background()
	customTTL := 5 * time.Minute

	// Execute
	err := adapter.Set(ctx, "ttl-key", "value", WithTTL(customTTL))
	require.NoError(t, err)

	// Verify
	ttl := mockRedis.TTL("ttl-key")
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, customTTL)
}

func TestRedisAdapter_HealthCheck(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*miniredis.Miniredis)
		wantErr bool
	}{
		{
			name:    "healthy",
			setup:   func(*miniredis.Miniredis) {},
			wantErr: false,
		},
		{
			name:    "unhealthy",
			setup:   func(m *miniredis.Miniredis) { m.Close() },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, mockRedis := setupTestRedis(t)
			defer adapter.Close()

			// Setup
			tt.setup(mockRedis)

			// Execute & Verify
			err := adapter.HealthCheck(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisAdapter_Type(t *testing.T) {
	adapter, mockRedis := setupTestRedis(t)
	defer adapter.Close()
	defer mockRedis.Close()

	assert.Equal(t, CacheRedis, adapter.Type())
}
