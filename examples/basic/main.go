//go:build ignore

// Basic example demonstrating nyro with the in-memory adapter.
// Run with: go run examples/basic/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	cache "github.com/vinaycharlie01/nyro"
	_ "github.com/vinaycharlie01/nyro/adapters/memory" // registers the Memory adapter
)

func main() {
	// Create an in-memory cache instance (no external dependencies needed).
	c, err := cache.New(cache.CacheMemory, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// Set a value with a 5-second TTL.
	if err := c.Set(ctx, "greeting", "hello, nyro!", cache.WithExpiration(5*time.Second)); err != nil {
		log.Fatal(err)
	}

	// Retrieve the value.
	val, err := c.Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: %v\n", val)

	// Check existence.
	exists, _ := c.Exists(ctx, "greeting")
	fmt.Printf("Exists: %v\n", exists)

	// GetOrSet — loader called only on a miss.
	val, err = c.GetOrSet(ctx, "computed", func() (any, error) {
		fmt.Println("  [loader] cache miss — computing value...")
		return "expensive result", nil
	}, cache.WithExpiration(time.Minute))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GetOrSet (miss): %v\n", val)

	// Second call — loader is NOT called (cache hit).
	val, _ = c.GetOrSet(ctx, "computed", func() (any, error) {
		fmt.Println("  [loader] this should NOT print")
		return nil, nil
	})
	fmt.Printf("GetOrSet (hit):  %v\n", val)

	// GetMulti — batch retrieval.
	if err := c.Set(ctx, "a", 1, cache.WithExpiration(time.Minute)); err != nil {
		log.Fatal(err)
	}
	if err := c.Set(ctx, "b", 2, cache.WithExpiration(time.Minute)); err != nil {
		log.Fatal(err)
	}
	results, errs := c.GetMulti(ctx, "a", "b", "missing")
	for k, v := range results {
		fmt.Printf("GetMulti %s: %v\n", k, v)
	}
	for k, e := range errs {
		fmt.Printf("GetMulti %s error: %v\n", k, e)
	}

	// Health check and stats.
	healthy := c.HealthCheck(ctx)
	stats := c.GetStats()
	fmt.Printf("Healthy: %v, Stats: %+v\n", healthy, stats)

	// Delete a key.
	if err := c.Delete(ctx, "greeting"); err != nil {
		log.Fatal(err)
	}
	_, err = c.Get(ctx, "greeting")
	fmt.Printf("After delete — err: %v\n", err) // cache: key not found
}
