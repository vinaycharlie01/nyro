# Architecture

nyro implements the **Hexagonal Architecture** (also called Ports & Adapters) pattern.
This document explains the design decisions and how the layers fit together.

## Hexagonal Architecture Overview

```
                     ┌───────────────────────────────────────┐
                     │           APPLICATION              │
                     │   ┌─────────────────────────┐   │
                     │   │    DOMAIN (Port)         │   │
                     │   │                         │   │
                     │   │   cache.Cache (iface)   │   │
                     │   │   TypedCache[T]         │   │
                     │   │   Options               │   │
                     │   │   Stats, Errors         │   │
                     │   └─────────────────────────┘   │
                     └───────────────────────────────────────┘
                           ↑             ↑             ↑
               ┌─────────┘     ┌─────────┘     ┌─────────┘
      ┌───────┴─────┐ ┌──────┴─────┐ ┌──────┴─────┐
      │  adapters  │ │  adapters  │ │  adapters  │
      │   /redis   │ │  /valkey   │ │  /memory   │
      └───────┬─────┘ └──────┬─────┘ └──────┬─────┘
               ↓             ↓             ↓
      ┌───────┴─────┐ ┌──────┴─────┐ ┌──────┴─────┐
      │ stores/    │ │ stores/    │ │ sync.Map   │
      │ redis      │ │ valkey     │ │ + GC +     │
      └─────────────┘ └─────────────┘ │ singleflt  │
                                          └─────────────┘
```

## Layer Responsibilities

### Domain / Port (`package cache`)

The root package is the **port** — the only stable API surface.

- `Cache` interface: `Get`, `Set`, `Delete`, `Exists`, `GetMulti`, `GetOrSet`, `Clear`, `HealthCheck`, `GetStats`, `Close`
- `TypedCache[T]`: generic wrapper that eliminates `any`-based type assertions everywhere
- `Decode[T]`: JSON-aware value decoding used by `TypedCache[T]`
- `Stats`: backend status snapshot returned by `GetStats()`
- `ErrNotFound`, `ErrBackendUnavailable`: sentinel errors for callers to match on
- `Register(CacheType, Factory)`: registry function for adapters
- `New(CacheType, config) (Cache, error)`: creates a registered adapter by type

**Rule**: no infrastructure imports in this package. It depends only on the Go standard library.

### Driven Adapters (`adapters/*`)

Each adapter is an independent Go package that:

1. Imports the domain port (`cache "github.com/vinaycharlie01/nyro"`)
2. Imports its backing store (`stores/redis`, `stores/valkey`, or pure `sync.Map`)
3. Registers itself in `init()` so a side-effect import is enough to activate it
4. Implements the full `cache.Cache` interface

#### Redis adapter (`adapters/redis`)

- Wraps `stores/redis` which holds the `go-redis/v9` client
- `effectiveTTL()`: cascades from requested TTL → default TTL → 24h fallback
- Distributed locking via `stores/redis.DistributedLocker` for `GetOrSet`

#### Valkey adapter (`adapters/valkey`)

- Wraps `stores/valkey` which holds the `valkey-go` client
- Supports client-side caching via `WithClientSideCaching()` option
- Pings the server before returning from `New()` to fail-fast

#### Memory adapter (`adapters/memory`)

- Pure in-memory with `sync.RWMutex`-guarded `map[string]entry`
- **TTL**: `entry.expiresAt` checked on every read; expired entries return `ErrNotFound`
- **Background GC**: configurable interval goroutine sweeps and removes expired entries (default: 1 minute)
- **Singleflight**: `golang.org/x/sync/singleflight` deduplicates concurrent `GetOrSet` calls for the same key, preventing thundering-herd cache stampedes
- **Safe shutdown**: `sync.Once` ensures `Close()` stops the GC goroutine exactly once

### Infrastructure (`stores/*`)

Stores are the low-level backend clients. They are **not** part of the `cache.Cache` interface —
adapters translate between the store's native API and the port's contract.

- `stores/store.go`: `Store` and `DistributedLocker` interfaces
- `stores/redis`: full `go-redis/v9` client, distributed locking with backoff, heartbeat lease renewal
- `stores/valkey`: `valkey-go` client

### Configuration (`config/`)

- `Config`: top-level config (backend type, Redis/Valkey/Memory sub-configs)
- `RedisConfig`, `ValkeyConfig`, `MemoryConfig`: backend-specific options
- `EntityCacheConfig`: per-entity TTL, key prefix, enabled flag
- `EntityConfigManager`: loads entity config from YAML or environment variables via `caarlos0/env`

### Application Facade (`client/`)

The `client.Client` embeds `cache.Cache` and adds entity-level awareness:
- Resolves per-entity TTL and key prefix from `EntityCacheConfig`
- Exposes `GetEntityConfig(entity string)` and `IsEntityEnabled(entity string)`

### Internal Utilities (`internal/keyutil`)

`keyutil.ToString` converts cache keys of any supported type to `string`:
- Passthrough for `string`
- `strconv.FormatInt`/`FormatUint` for integer types
- Falls back to `fmt.Stringer` interface then `fmt.Sprintf("%v", key)`

This package is `internal/` so it cannot be imported by consumers of the module.

## Auto-Registration Pattern

Adapters register themselves using Go's `init()` function:

```go
// adapters/redis/adapter.go
func init() {
    cache.Register(cache.CacheRedis, func(cfg any) (cache.Cache, error) {
        redisCfg, ok := cfg.(*nyroconfig.RedisConfig)
        // ...
        return New(*redisCfg)
    })
}
```

Callers activate an adapter with a blank import:

```go
import _ "github.com/vinaycharlie01/nyro/adapters/redis"
```

This pattern means:
- The binary only links the adapters it actually uses
- Adding a new backend requires zero changes to the core package
- The registry is a `sync.Map` — safe for concurrent access

## Concurrency Model

| Component | Mechanism |
|-----------|----------|
| Memory adapter reads | `sync.RWMutex` (multiple concurrent readers) |
| Memory adapter writes | Exclusive `sync.Mutex` |
| Memory GC goroutine | Stopped via `context.WithCancel` + `sync.Once` in `Close()` |
| GetOrSet deduplication | `singleflight.Group` (one call in-flight per key) |
| Adapter registry | `sync.Map` (lock-free reads after startup) |

## Extending nyro

To add a new cache backend:

1. Create `adapters/youradapter/adapter.go` with `package youradapter`
2. Implement all methods of `cache.Cache`
3. Add `init()` registration:
   ```go
   func init() {
       cache.Register(cache.CacheType("youradapter"), func(cfg any) (cache.Cache, error) {
           return New(cfg.(*YourConfig))
       })
   }
   ```
4. Write tests in `adapters/youradapter/adapter_test.go`
5. Side-effect import in your application: `import _ "github.com/vinaycharlie01/nyro/adapters/youradapter"`

No changes to the core package, the registry, or existing adapters are required.
