//go:build ignore

// TypedCache example demonstrating type-safe generic cache access.
// Run with: go run examples/typed/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	cache "github.com/vinaycharlie01/nyro"
	_ "github.com/vinaycharlie01/nyro/adapters/memory"
)

// Product is an example domain type.
type Product struct {
	ID    int
	Name  string
	Price float64
	Tags  []string
}

// loadProduct simulates a database call.
func loadProduct(id int) (Product, error) {
	fmt.Printf("  [db] loading product %d...\n", id)
	return Product{
		ID:    id,
		Name:  fmt.Sprintf("Product %d", id),
		Price: float64(id) * 9.99,
		Tags:  []string{"new", "featured"},
	}, nil
}

func main() {
	// Create a base cache and wrap it in a type-safe TypedCache[Product].
	base, err := cache.New(cache.CacheMemory, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer base.Close()

	tc := cache.NewTypedCache[Product](base)
	ctx := context.Background()
	ttl := cache.WithExpiration(time.Hour)

	// Set a Product — no serialization ceremony.
	tc.Set(ctx, "product:1", Product{ID: 1, Name: "Widget", Price: 9.99}, ttl)

	// Get a Product — fully typed, no type assertion required.
	p, err := tc.Get(ctx, "product:1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: %+v\n", p)

	// GetOrSet — loader returns Product directly (no any wrapping).
	p2, err := tc.GetOrSet(ctx, "product:2", func() (Product, error) {
		return loadProduct(2)
	}, ttl)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GetOrSet (miss): %+v\n", p2)

	// Second call hits the cache — loader not invoked.
	p2, _ = tc.GetOrSet(ctx, "product:2", func() (Product, error) {
		fmt.Println("  [db] this should NOT print")
		return Product{}, nil
	})
	fmt.Printf("GetOrSet (hit):  %+v\n", p2)

	// Pre-warm a few more entries.
	for i := 3; i <= 5; i++ {
		tc.Set(ctx, fmt.Sprintf("product:%d", i), Product{
			ID: i, Name: fmt.Sprintf("Item %d", i), Price: float64(i) * 4.5,
		}, ttl)
	}

	// GetMulti — returns map[string]Product and map[string]error.
	keys := []string{"product:1", "product:2", "product:3", "product:4", "product:5", "product:99"}
	products, errs := tc.GetMulti(ctx, keys...)
	fmt.Println("\nGetMulti results:")
	for _, k := range keys {
		if p, ok := products[k]; ok {
			fmt.Printf("  ✓ %s: %+v\n", k, p)
		} else if e, ok := errs[k]; ok {
			fmt.Printf("  ✗ %s: %v\n", k, e)
		}
	}
}
