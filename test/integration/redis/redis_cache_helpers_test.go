//go:build integration
// +build integration

package redis_integration_test

import (
	"sync"
)

// runConcurrent executes fn concurrently n times with synchronized start
func runConcurrent(n int, fn func(idx int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			fn(idx)
		}(i)
	}

	close(start)
	wg.Wait()
}
