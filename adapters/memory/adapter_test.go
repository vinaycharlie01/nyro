package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro"
	nyroconfig "github.com/vinaycharlie01/nyro/config"

	memoryadapter "github.com/vinaycharlie01/nyro/adapters/memory"
)

func newTestAdapter() *memoryadapter.Adapter {
	return memoryadapter.New(nyroconfig.MemoryConfig{
		DefaultTTL: time.Minute,
		GCInterval: time.Hour, // avoid GC interference during tests
	})
}

func TestAdapter_SetGet(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "key1", "value1"))

	got, err := a.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "value1", got)
}

func TestAdapter_GetMiss(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	_, err := a.Get(ctx, "nonexistent")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestAdapter_Delete(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k", "v"))
	require.NoError(t, a.Delete(ctx, "k"))

	_, err := a.Get(ctx, "k")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestAdapter_Exists(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	ok, err := a.Exists(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, a.Set(ctx, "present", "v"))

	ok, err = a.Exists(ctx, "present")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAdapter_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	a := memoryadapter.New(nyroconfig.MemoryConfig{GCInterval: time.Hour})
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k", "v", cache.WithExpiration(50*time.Millisecond)))
	time.Sleep(100 * time.Millisecond)

	_, err := a.Get(ctx, "k")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestAdapter_Clear(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k1", "v1"))
	require.NoError(t, a.Set(ctx, "k2", "v2"))
	require.NoError(t, a.Clear(ctx))

	_, err := a.Get(ctx, "k1")
	assert.ErrorIs(t, err, cache.ErrNotFound)
}

func TestAdapter_GetOrSet(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	calls := 0
	loader := func(_ context.Context) (any, error) {
		calls++

		return "loaded", nil
	}

	v, err := a.GetOrSet(ctx, "k", loader)
	require.NoError(t, err)
	assert.Equal(t, "loaded", v)

	v, err = a.GetOrSet(ctx, "k", loader)
	require.NoError(t, err)
	assert.Equal(t, "loaded", v)
	assert.Equal(t, 1, calls, "loader should only be called once")
}

func TestAdapter_GetOrSet_LoaderError(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	_, err := a.GetOrSet(ctx, "k", func(_ context.Context) (any, error) {
		return nil, errors.New("loader error")
	})
	assert.Error(t, err)
}

func TestAdapter_GetMulti(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k1", "v1"))
	require.NoError(t, a.Set(ctx, "k2", "v2"))

	res, err := a.GetMulti(ctx, []any{"k1", "k2", "k3"})
	require.NoError(t, err)
	assert.Equal(t, "v1", res["k1"])
	assert.Equal(t, "v2", res["k2"])
	assert.Nil(t, res["k3"])
}

func TestAdapter_GetStats(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	stats, err := a.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "memory", stats.Type)
	assert.True(t, stats.Connected)
}

func TestAdapter_HealthCheck(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter()
	defer a.Close() //nolint:errcheck

	assert.NoError(t, a.HealthCheck(ctx))
}
