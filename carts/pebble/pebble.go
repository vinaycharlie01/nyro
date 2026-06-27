// Package pebble provides a Pebble-backed cart implementation.
package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	cart "github.com/vinaycharlie01/nyro/carts"
)

const (
	// DefaultPath is the default directory path for Pebble database.
	DefaultPath = "./pebble_cache"
)

// PebbleCart implements cart.Cart for Pebble.
type PebbleCart struct {
	db *pebble.DB
}

// CacheEntry represents a cache entry with expiration.
type CacheEntry struct {
	Value      any       `json:"value"`
	Expiration time.Time `json:"expiration"`
}

// PebbleCartOption is a functional option for configuring PebbleCart.
type PebbleCartOption func(*PebbleCart)

// NewPebble creates a new Pebble cart backend.
func NewPebble(db *pebble.DB, opts ...PebbleCartOption) *PebbleCart {
	pc := &PebbleCart{
		db: db,
	}

	for _, opt := range opts {
		opt(pc)
	}

	return pc
}

// Get retrieves a value from the cache.
func (pc *PebbleCart) Get(ctx context.Context, key string) (any, error) {
	value, closer, err := pc.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, cart.NotFoundWithCause(err)
		}
		return nil, fmt.Errorf("pebble: get failed: %w", err)
	}
	defer closer.Close()

	var entry CacheEntry
	if err := json.Unmarshal(value, &entry); err != nil {
		return nil, fmt.Errorf("pebble: unmarshal failed: %w", err)
	}

	// Check expiration
	if time.Now().After(entry.Expiration) {
		// Delete expired entry
		_ = pc.db.Delete([]byte(key), pebble.Sync)
		return nil, cart.NotFoundWithCause(errors.New("key expired"))
	}

	return entry.Value, nil
}

// Set stores a value in the cache with expiration.
func (pc *PebbleCart) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	entry := CacheEntry{
		Value:      value,
		Expiration: time.Now().Add(expiration),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("pebble: marshal failed: %w", err)
	}

	if err := pc.db.Set([]byte(key), data, pebble.Sync); err != nil {
		return fmt.Errorf("pebble: set failed: %w", err)
	}

	return nil
}

// Delete removes a value from the cache.
func (pc *PebbleCart) Delete(ctx context.Context, key string) error {
	if err := pc.db.Delete([]byte(key), pebble.Sync); err != nil {
		return fmt.Errorf("pebble: delete failed: %w", err)
	}

	return nil
}

// Exists checks if a key exists in the cache.
func (pc *PebbleCart) Exists(ctx context.Context, key string) (bool, error) {
	_, closer, err := pc.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("pebble: exists check failed: %w", err)
	}
	closer.Close()

	return true, nil
}

// GetMulti retrieves multiple values from the cache.
func (pc *PebbleCart) GetMulti(ctx context.Context, keys []string) (map[string]any, error) {
	result := make(map[string]any, len(keys))

	for _, key := range keys {
		value, err := pc.Get(ctx, key)
		if err != nil {
			var notFound *cart.NotFound
			if errors.As(err, &notFound) {
				continue
			}
			return nil, err
		}
		result[key] = value
	}

	return result, nil
}

// SetMulti stores multiple values in the cache.
func (pc *PebbleCart) SetMulti(ctx context.Context, items map[string]any, expiration time.Duration) error {
	batch := pc.db.NewBatch()
	defer batch.Close()

	for key, value := range items {
		entry := CacheEntry{
			Value:      value,
			Expiration: time.Now().Add(expiration),
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("pebble: marshal failed: %w", err)
		}

		if err := batch.Set([]byte(key), data, pebble.Sync); err != nil {
			return fmt.Errorf("pebble: batch set failed: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble: batch commit failed: %w", err)
	}

	return nil
}

// DeleteMulti removes multiple values from the cache.
func (pc *PebbleCart) DeleteMulti(ctx context.Context, keys []string) error {
	batch := pc.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete([]byte(key), pebble.Sync); err != nil {
			return fmt.Errorf("pebble: batch delete failed: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble: batch commit failed: %w", err)
	}

	return nil
}

// Clear removes all entries from the cache.
func (pc *PebbleCart) Clear(ctx context.Context) error {
	// Delete all keys by iterating
	iter, err := pc.db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("pebble: iterator creation failed: %w", err)
	}
	defer iter.Close()

	batch := pc.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), pebble.Sync); err != nil {
			return fmt.Errorf("pebble: batch delete failed: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("pebble: iteration failed: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble: batch commit failed: %w", err)
	}

	return nil
}

// HealthCheck verifies the Pebble database is accessible.
func (pc *PebbleCart) HealthCheck(ctx context.Context) error {
	// Try a simple operation
	testKey := []byte("__health_check__")
	if err := pc.db.Set(testKey, []byte("ok"), pebble.Sync); err != nil {
		return fmt.Errorf("pebble: health check failed: %w", err)
	}

	_ = pc.db.Delete(testKey, pebble.Sync)

	return nil
}

// Close closes the Pebble database.
func (pc *PebbleCart) Close() error {
	return pc.db.Close()
}
