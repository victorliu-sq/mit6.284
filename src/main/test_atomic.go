package main

import (
	"sync"
	"sync/atomic"
)

func main() {
	var a int64
	atomic.StoreInt64(&a, 0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(x int) {
			atomic.AddInt64(&a, 1)
			temp := atomic.LoadInt64(&a)
			println(temp)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
