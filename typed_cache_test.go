package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	_ "github.com/vinaycharlie01/nyro/adapters/memory"
	"github.com/vinaycharlie01/nyro/config"
)

// TestUser is a test domain model.
type TestUser struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func newTestCache(t *testing.T) cache.Cache {
	t.Helper()
	c, err := config.New(config.CacheMemory, &config.MemoryConfig{
		DefaultTTL: 1 * time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestTypedCache_Get_Set(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice", Email: "alice@example.com"}

	err := typedCache.Set(ctx, "user:1", user, cache.WithTTL(5*time.Minute))
	require.NoError(t, err)

	retrieved, err := typedCache.Get(ctx, "user:1")
	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved.ID)
	assert.Equal(t, user.Name, retrieved.Name)
	assert.Equal(t, user.Email, retrieved.Email)
}

func TestTypedCache_GetOrSet(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()
	user := &TestUser{ID: 2, Name: "Bob", Email: "bob@example.com"}

	retrieved, err := typedCache.GetOrSet(ctx, "user:2", func(ctx context.Context) (*TestUser, error) {
		return user, nil
	}, cache.WithTTL(10*time.Minute))

	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved.ID)
	assert.Equal(t, user.Name, retrieved.Name)

	// Second call should return the cached value, not invoke the loader.
	retrieved2, err := typedCache.GetOrSet(ctx, "user:2", func(ctx context.Context) (*TestUser, error) {
		return &TestUser{ID: 999, Name: "Should not be called"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, user.ID, retrieved2.ID)
}

func TestTypedCache_GetMulti_SetMulti(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()
	users := map[any]*TestUser{
		"user:1": {ID: 1, Name: "Alice", Email: "alice@example.com"},
		"user:2": {ID: 2, Name: "Bob", Email: "bob@example.com"},
	}

	err := typedCache.SetMulti(ctx, users, cache.WithTTL(5*time.Minute))
	require.NoError(t, err)

	keys := []any{"user:1", "user:2"}
	retrieved, err := typedCache.GetMulti(ctx, keys)
	require.NoError(t, err)
	assert.Len(t, retrieved, 2)
	assert.Equal(t, users["user:1"].Name, retrieved["user:1"].Name)
	assert.Equal(t, users["user:2"].Name, retrieved["user:2"].Name)
}

func TestTypedCache_Delete_DeleteMulti(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice"}

	_ = typedCache.Set(ctx, "user:1", user)

	err := typedCache.Delete(ctx, "user:1")
	require.NoError(t, err)

	exists, _ := typedCache.Exists(ctx, "user:1")
	assert.False(t, exists)

	users := map[any]*TestUser{
		"user:1": {ID: 1, Name: "Alice"},
		"user:2": {ID: 2, Name: "Bob"},
		"user:3": {ID: 3, Name: "Charlie"},
	}
	_ = typedCache.SetMulti(ctx, users)

	keys := []any{"user:1", "user:2", "user:3"}
	err = typedCache.DeleteMulti(ctx, keys)
	require.NoError(t, err)

	for _, key := range keys {
		exists, _ := typedCache.Exists(ctx, key)
		assert.False(t, exists)
	}
}

func TestTypedCache_Exists(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()
	user := &TestUser{ID: 1, Name: "Alice"}

	exists, err := typedCache.Exists(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, exists)

	_ = typedCache.Set(ctx, "user:1", user)

	exists, err = typedCache.Exists(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestTypedCache_Clear(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	ctx := context.Background()

	_ = typedCache.Set(ctx, "user:1", &TestUser{ID: 1, Name: "Alice"})
	_ = typedCache.Set(ctx, "user:2", &TestUser{ID: 2, Name: "Bob"})

	err := typedCache.Clear(ctx)
	require.NoError(t, err)

	exists1, _ := typedCache.Exists(ctx, "user:1")
	exists2, _ := typedCache.Exists(ctx, "user:2")
	assert.False(t, exists1)
	assert.False(t, exists2)
}

func TestTypedCache_SliceTypes(t *testing.T) {
	typedCache := cache.NewTypedCache[[]*TestUser](newTestCache(t))

	ctx := context.Background()
	users := []*TestUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com"},
		{ID: 2, Name: "Bob", Email: "bob@example.com"},
	}

	err := typedCache.Set(ctx, "users:active", users)
	require.NoError(t, err)

	retrieved, err := typedCache.Get(ctx, "users:active")
	require.NoError(t, err)
	assert.Len(t, retrieved, 2)
	assert.Equal(t, users[0].Name, retrieved[0].Name)
	assert.Equal(t, users[1].Name, retrieved[1].Name)
}

func TestTypedCache_PrimitiveTypes(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		typedCache := cache.NewTypedCache[string](newTestCache(t))
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:1", "hello")
		retrieved, err := typedCache.Get(ctx, "key:1")
		require.NoError(t, err)
		assert.Equal(t, "hello", retrieved)
	})

	t.Run("int", func(t *testing.T) {
		typedCache := cache.NewTypedCache[int](newTestCache(t))
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:2", 42)
		retrieved, err := typedCache.Get(ctx, "key:2")
		require.NoError(t, err)
		assert.Equal(t, 42, retrieved)
	})

	t.Run("bool", func(t *testing.T) {
		typedCache := cache.NewTypedCache[bool](newTestCache(t))
		ctx := context.Background()

		_ = typedCache.Set(ctx, "key:3", true)
		retrieved, err := typedCache.Get(ctx, "key:3")
		require.NoError(t, err)
		assert.True(t, retrieved)
	})
}

func TestTypedCache_Unwrap(t *testing.T) {
	underlying := newTestCache(t)
	typedCache := cache.NewTypedCache[*TestUser](underlying)

	assert.Equal(t, underlying, typedCache.Unwrap())
}

func TestTypedCache_HealthCheck(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	err := typedCache.HealthCheck(context.Background())
	assert.NoError(t, err)
}

func TestTypedCache_GetStats(t *testing.T) {
	typedCache := cache.NewTypedCache[*TestUser](newTestCache(t))

	stats, err := typedCache.GetStats(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, string(config.CacheMemory), stats.Type)
	assert.True(t, stats.Connected)
}

func TestTypedCache_Close(t *testing.T) {
	c, err := config.New(config.CacheMemory, &config.MemoryConfig{DefaultTTL: time.Hour})
	require.NoError(t, err)

	typedCache := cache.NewTypedCache[*TestUser](c)

	err = typedCache.Set(context.Background(), "test-key", &TestUser{ID: 1, Name: "Test"})
	require.NoError(t, err)

	err = typedCache.Close()
	assert.NoError(t, err)
}
