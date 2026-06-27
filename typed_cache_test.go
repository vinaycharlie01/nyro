package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedisForTypedCache creates a RedisAdapter with miniredis for testing
func setupTestRedisForTypedCache(t *testing.T) (Cache, *miniredis.Miniredis) {
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

// TestUser is a test domain model
type TestUser struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func TestTypedCache_Get_Set(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice", Email: "alice@example.com"}

	// Test Set
	err := typedCache.Set(ctx, "user:1", user, WithTTL(5*time.Minute))
	require.NoError(t, err)

	// Test Get
	retrieved, err := typedCache.Get(ctx, "user:1")
	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved.ID)
	assert.Equal(t, user.Name, retrieved.Name)
	assert.Equal(t, user.Email, retrieved.Email)
}

func TestTypedCache_GetOrSet(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()
	user := &TestUser{ID: 2, Name: "Bob", Email: "bob@example.com"}

	// Test GetOrSet with type-safe loader
	retrieved, err := typedCache.GetOrSet(ctx, "user:2", func(ctx context.Context) (*TestUser, error) {
		return user, nil
	}, WithTTL(10*time.Minute))

	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved.ID)
	assert.Equal(t, user.Name, retrieved.Name)

	// Second call should return cached value
	retrieved2, err := typedCache.GetOrSet(ctx, "user:2", func(ctx context.Context) (*TestUser, error) {
		return &TestUser{ID: 999, Name: "Should not be called"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved2.ID) // Should still be original user
}

func TestTypedCache_GetMulti_SetMulti(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()
	users := map[any]*TestUser{
		"user:1": {ID: 1, Name: "Alice", Email: "alice@example.com"},
		"user:2": {ID: 2, Name: "Bob", Email: "bob@example.com"},
	}

	// Test SetMulti
	err := typedCache.SetMulti(ctx, users, WithTTL(5*time.Minute))
	require.NoError(t, err)

	// Test GetMulti
	keys := []any{"user:1", "user:2"}
	retrieved, err := typedCache.GetMulti(ctx, keys)
	require.NoError(t, err)
	assert.Len(t, retrieved, 2)
	assert.Equal(t, users["user:1"].Name, retrieved["user:1"].Name)
	assert.Equal(t, users["user:2"].Name, retrieved["user:2"].Name)
}

func TestTypedCache_Delete_DeleteMulti(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice"}

	// Set a value
	_ = typedCache.Set(ctx, "user:1", user)

	// Test Delete
	err := typedCache.Delete(ctx, "user:1")
	require.NoError(t, err)

	// Verify deleted
	exists, _ := typedCache.Exists(ctx, "user:1")
	assert.False(t, exists)

	// Set multiple values
	users := map[any]*TestUser{
		"user:1": {ID: 1, Name: "Alice"},
		"user:2": {ID: 2, Name: "Bob"},
		"user:3": {ID: 3, Name: "Charlie"},
	}
	_ = typedCache.SetMulti(ctx, users)

	// Test DeleteMulti
	keys := []any{"user:1", "user:2", "user:3"}
	err = typedCache.DeleteMulti(ctx, keys)
	require.NoError(t, err)

	// Verify all deleted
	for _, key := range keys {
		exists, _ := typedCache.Exists(ctx, key)
		assert.False(t, exists)
	}
}

func TestTypedCache_Exists(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice"}

	// Key doesn't exist
	exists, err := typedCache.Exists(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, exists)

	// Set value
	_ = typedCache.Set(ctx, "user:1", user)

	// Key exists
	exists, err = typedCache.Exists(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestTypedCache_Clear(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	ctx := context.Background()

	// Set some values
	_ = typedCache.Set(ctx, "user:1", &TestUser{ID: 1, Name: "Alice"})
	_ = typedCache.Set(ctx, "user:2", &TestUser{ID: 2, Name: "Bob"})

	// Clear
	err := typedCache.Clear(ctx)
	require.NoError(t, err)

	// Verify cleared
	exists1, _ := typedCache.Exists(ctx, "user:1")
	exists2, _ := typedCache.Exists(ctx, "user:2")
	assert.False(t, exists1)
	assert.False(t, exists2)
}

func TestTypedCache_SliceTypes(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[[]*TestUser](cache)

	ctx := context.Background()
	users := []*TestUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com"},
		{ID: 2, Name: "Bob", Email: "bob@example.com"},
	}

	// Set slice
	err := typedCache.Set(ctx, "users:active", users)
	require.NoError(t, err)

	// Get slice
	retrieved, err := typedCache.Get(ctx, "users:active")
	require.NoError(t, err)
	assert.Len(t, retrieved, 2)
	assert.Equal(t, users[0].Name, retrieved[0].Name)
	assert.Equal(t, users[1].Name, retrieved[1].Name)
}

func TestTypedCache_PrimitiveTypes(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		cache, mockRedis := setupTestRedisForTypedCache(t)
		defer cache.Close()
		defer mockRedis.Close()

		typedCache := NewTypedCache[string](cache)
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:1", "hello")
		retrieved, err := typedCache.Get(ctx, "key:1")
		require.NoError(t, err)
		assert.Equal(t, "hello", retrieved)
	})

	t.Run("int", func(t *testing.T) {
		cache, mockRedis := setupTestRedisForTypedCache(t)
		defer cache.Close()
		defer mockRedis.Close()

		typedCache := NewTypedCache[int](cache)
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:2", 42)
		retrieved, err := typedCache.Get(ctx, "key:2")
		require.NoError(t, err)
		assert.Equal(t, 42, retrieved)
	})

	t.Run("bool", func(t *testing.T) {
		cache, mockRedis := setupTestRedisForTypedCache(t)
		defer cache.Close()
		defer mockRedis.Close()

		typedCache := NewTypedCache[bool](cache)
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:3", true)
		retrieved, err := typedCache.Get(ctx, "key:3")
		require.NoError(t, err)
		assert.True(t, retrieved)
	})
}

func TestTypedCache_Unwrap(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	unwrapped := typedCache.Unwrap()
	assert.Equal(t, cache, unwrapped)
}

func TestTypedCache_HealthCheck(t *testing.T) {
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
			cache, mockRedis := setupTestRedisForTypedCache(t)
			defer cache.Close()

			typedCache := NewTypedCache[*TestUser](cache)

			// Setup
			tt.setup(mockRedis)

			// Execute & Verify
			err := typedCache.HealthCheck(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTypedCache_GetStats(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	defer cache.Close()
	defer mockRedis.Close()

	typedCache := NewTypedCache[*TestUser](cache)

	stats, err := typedCache.GetStats(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, string(CacheRedis), stats.Type)
	assert.True(t, stats.Connected)
}

func TestTypedCache_Close(t *testing.T) {
	cache, mockRedis := setupTestRedisForTypedCache(t)
	typedCache := NewTypedCache[*TestUser](cache)

	// Verify cache is working before close
	err := typedCache.Set(context.Background(), "test-key", &TestUser{ID: 1, Name: "Test"})
	require.NoError(t, err)

	// Close should not return error
	err = typedCache.Close()
	assert.NoError(t, err)

	// Close miniredis after cache is closed
	mockRedis.Close()
}
