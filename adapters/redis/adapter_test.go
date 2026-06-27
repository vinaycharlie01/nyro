package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nyroconfig "github.com/vinaycharlie01/nyro/config"

	redisadapter "github.com/vinaycharlie01/nyro/adapters/redis"
)

func newTestAdapter(t *testing.T) *redisadapter.Adapter {
	t.Helper()
	mr := miniredis.RunT(t)

	adapter, err := redisadapter.New(nyroconfig.RedisConfig{
		Addr:       mr.Addr(),
		DefaultTTL: time.Minute,
	})
	require.NoError(t, err)

	return adapter
}

func TestAdapter_SetGet(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "key1", "value1"))

	got, err := a.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "value1", got)
}

func TestAdapter_GetMiss(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	_, err := a.Get(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestAdapter_Delete(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "key1", "value1"))
	require.NoError(t, a.Delete(ctx, "key1"))

	exists, err := a.Exists(ctx, "key1")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestAdapter_Exists(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	exists, err := a.Exists(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, a.Set(ctx, "present", "v"))

	exists, err = a.Exists(ctx, "present")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestAdapter_GetMulti(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k1", "v1"))
	require.NoError(t, a.Set(ctx, "k2", "v2"))

	res, err := a.GetMulti(ctx, []any{"k1", "k2", "k3"})
	require.NoError(t, err)
	assert.Equal(t, "v1", res["k1"])
	assert.Equal(t, "v2", res["k2"])
	assert.Nil(t, res["k3"])
}

func TestAdapter_HealthCheck(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	assert.NoError(t, a.HealthCheck(ctx))
}

func TestAdapter_GetStats(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	stats, err := a.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "redis", stats.Type)
	assert.True(t, stats.Connected)
}

func TestAdapter_Clear(t *testing.T) {
	ctx := context.Background()
	a := newTestAdapter(t)
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "k1", "v1"))
	require.NoError(t, a.Clear(ctx))

	exists, err := a.Exists(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, exists)
}
