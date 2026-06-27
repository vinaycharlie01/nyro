//go:build integration
// +build integration

package testcontainers

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	cache "github.com/vinaycharlie01/nyro"
	redisadapter "github.com/vinaycharlie01/nyro/adapters/redis"
	nyroconfig "github.com/vinaycharlie01/nyro/config"
)

// =============================================================================
// MSSQL Testcontainer Setup
// =============================================================================

// MSSQLContainer holds MSSQL container instance and connection details
// Provides a fluent builder API for configuring SQL Server testcontainers
type MSSQLContainer struct {
	container        *mssql.MSSQLServerContainer
	connectionString string
	image            string
	password         string
	db               *sql.DB
	sqlData          []byte
}

// GetDB returns the database connection
func (m *MSSQLContainer) GetDB() *sql.DB {
	return m.db
}

// GetConnectionString returns the connection string
func (m *MSSQLContainer) GetConnectionString() string {
	return m.connectionString
}

// WithInitSQL sets the initial SQL data to execute on container startup
// This is useful for loading test data or schema
func (m *MSSQLContainer) WithInitSQL(sqlData []byte) *MSSQLContainer {
	m.sqlData = sqlData
	return m
}

// WithConnectionString sets a custom connection string (optional)
// If not set, the container will generate one automatically
func (m *MSSQLContainer) WithConnectionString(conn string) *MSSQLContainer {
	m.connectionString = conn
	return m
}

// WithImage sets a custom SQL Server image (optional)
// Default: mcr.microsoft.com/mssql/server:2022-latest
func (m *MSSQLContainer) WithImage(image string) *MSSQLContainer {
	m.image = image
	return m
}

// WithPassword sets a custom SA password (optional)
// Default: YourStrong@Passw0rd
func (m *MSSQLContainer) WithPassword(password string) *MSSQLContainer {
	m.password = password
	return m
}

// NewMSSQLContainer creates a new MSSQL container builder
func NewMSSQLContainer() *MSSQLContainer {
	return &MSSQLContainer{
		image:    "mcr.microsoft.com/mssql/server:2022-latest",
		password: "YourStrong@Passw0rd",
	}
}

// SetupMSSQLContainer starts a SQL Server container with the configured options
// Returns the container instance with connection details and database connection
func (m *MSSQLContainer) SetupMSSQLContainer(t testing.TB) *MSSQLContainer {
	t.Helper()

	// Use test context for better lifecycle management
	ctx := context.Background()
	if testCtx, ok := t.(interface{ Context() context.Context }); ok {
		ctx = testCtx.Context()
	}

	// Build container options
	opts := []testcontainers.ContainerCustomizer{
		mssql.WithAcceptEULA(),
		mssql.WithPassword(m.password),
	}

	// Add custom image if specified
	if m.image != "" && m.image != "mcr.microsoft.com/mssql/server:2022-latest" {
		opts = append(opts, testcontainers.WithImage(m.image))
	}

	// Add init SQL if provided
	if m.sqlData != nil {
		opts = append(opts, mssql.WithInitSQL(bytes.NewReader(m.sqlData)))
	}

	// Start MSSQL container using official testcontainers module
	mssqlContainer, err := mssql.Run(ctx, m.image, opts...)
	require.NoError(t, err, "failed to start MSSQL container")

	// Get connection string
	connStr, err := mssqlContainer.ConnectionString(ctx)
	require.NoError(t, err, "failed to get MSSQL connection string")

	// Use custom connection string if provided
	if m.connectionString != "" {
		connStr = m.connectionString
	}

	// Open database connection
	db, err := sql.Open("sqlserver", connStr)
	require.NoError(t, err, "failed to open MSSQL connection")

	// Test connection with retries
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		err = db.PingContext(ctx)
		if err == nil {
			break
		}
		if i == maxRetries-1 {
			require.NoError(t, err, "failed to ping MSSQL after %d retries", maxRetries)
		}
		time.Sleep(time.Second)
	}

	// Configure connection pool for optimal performance
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Update container fields
	m.container = mssqlContainer
	m.connectionString = connStr
	m.db = db

	// Register cleanup
	t.Cleanup(func() {
		if m.db != nil {
			m.db.Close()
		}
		if m.container != nil {
			if err := m.container.Terminate(ctx); err != nil {
				t.Logf("Warning: failed to terminate MSSQL container: %v", err)
			}
		}
	})

	t.Logf("✅ MSSQL container started: %s", connStr)
	return m
}

// =============================================================================
// Redis Testcontainer Setup
// =============================================================================

// RedisContainer holds Redis container instance and connection details.
// Provides access to Redis client and cache adapter for testing.
type RedisContainer struct {
	container    *redis.RedisContainer
	addr         string
	image        string
	client       *goredis.Client
	cacheAdapter cache.Cache
}

// GetAddr returns the Redis address
func (r *RedisContainer) GetAddr() string {
	return r.addr
}

// GetClient returns the Redis client.
func (r *RedisContainer) GetClient() *goredis.Client {
	return r.client
}

// GetCacheAdapter returns the cache adapter.
func (r *RedisContainer) GetCacheAdapter() cache.Cache {
	return r.cacheAdapter
}

// WithImage sets a custom Redis image (optional).
// Default: redis:7-alpine.
func (r *RedisContainer) WithImage(image string) *RedisContainer {
	r.image = image
	return r
}

// NewRedisContainer creates a new Redis container builder.
func NewRedisContainer() *RedisContainer {
	return &RedisContainer{
		image: "redis:7-alpine",
	}
}

// SetupRedisContainer starts a Redis container with the configured options.
// Returns container instance with Redis address and client.
func (r *RedisContainer) SetupRedisContainer(t testing.TB) *RedisContainer {
	t.Helper()

	// Use test context for better lifecycle management
	ctx := context.Background()
	if testCtx, ok := t.(interface{ Context() context.Context }); ok {
		ctx = testCtx.Context()
	}

	// Start Redis container using official testcontainers module
	redisContainer, err := redis.Run(ctx,
		r.image,
		redis.WithSnapshotting(10, 1),
		redis.WithLogLevel(redis.LogLevelVerbose),
	)
	require.NoError(t, err, "failed to start Redis container")

	// Get connection details using Host() + MappedPort() for correct format
	host, err := redisContainer.Host(ctx)
	require.NoError(t, err, "failed to get Redis host")

	port, err := redisContainer.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err, "failed to get Redis port")

	// Build address in format "host:port" (not redis://host:port)
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create Redis client
	redisClient := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
	})

	// Test connection
	err = redisClient.Ping(ctx).Err()
	require.NoError(t, err, "failed to ping Redis")

	// Create cache adapter
	cacheAdapter, err := redisadapter.New(nyroconfig.RedisConfig{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
		DefaultTTL:   30 * time.Minute,
		LockTTL:      10 * time.Second,
		LockMaxWait:  3 * time.Second,
	})
	require.NoError(t, err, "failed to create cache adapter")

	// Update container fields
	r.container = redisContainer
	r.addr = addr
	r.client = redisClient
	r.cacheAdapter = cacheAdapter

	// Register cleanup
	t.Cleanup(func() {
		if r.cacheAdapter != nil {
			_ = r.cacheAdapter.Close()
		}
		if r.client != nil {
			_ = r.client.Close()
		}
		if r.container != nil {
			if err := r.container.Terminate(ctx); err != nil {
				t.Logf("Warning: failed to terminate Redis container: %v", err)
			}
		}
	})

	t.Logf("✅ Redis container started: %s", addr)
	return r
}

// SetupRedisContainer is a convenience function for quick Redis setup.
// For more control, use NewRedisContainer().WithImage(...).SetupRedisContainer(t).
func SetupRedisContainer(t testing.TB) *RedisContainer {
	return NewRedisContainer().SetupRedisContainer(t)
}

// SetupMSSQLContainer is a convenience function for quick MSSQL setup.
// For more control, use NewMSSQLContainer().WithImage(...).WithInitSQL(...).SetupMSSQLContainer(t).
func SetupMSSQLContainer(t testing.TB) *MSSQLContainer {
	return NewMSSQLContainer().SetupMSSQLContainer(t)
}
