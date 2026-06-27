package valkey_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nyroconfig "github.com/vinaycharlie01/nyro/config"

	valkeyadapter "github.com/vinaycharlie01/nyro/adapters/valkey"
)

// TestAdapter_Integration tests require a live Valkey/Redis server on localhost:6379.
// Run with: VALKEY_ADDR=localhost:6379 go test ./adapters/valkey/...
func TestAdapter_SetGet(t *testing.T) {
	t.Skip("integration test: requires live Valkey server (set VALKEY_ADDR)")

	ctx := context.Background()
	a, err := valkeyadapter.New(nyroconfig.ValkeyConfig{Addr: "localhost:6379"})
	require.NoError(t, err)
	defer a.Close() //nolint:errcheck

	require.NoError(t, a.Set(ctx, "key1", "value1"))

	got, err := a.Get(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, "value1", got)
}

func TestAdapter_HealthCheck(t *testing.T) {
	t.Skip("integration test: requires live Valkey server (set VALKEY_ADDR)")

	ctx := context.Background()
	a, err := valkeyadapter.New(nyroconfig.ValkeyConfig{Addr: "localhost:6379"})
	require.NoError(t, err)
	defer a.Close() //nolint:errcheck

	assert.NoError(t, a.HealthCheck(ctx))
}
