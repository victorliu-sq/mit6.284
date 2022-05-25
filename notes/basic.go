package main

import (
	"sync"
)

func main() {
	var m sync.Mutex
	p := []int{}
	finished := 0

	cv := sync.NewCond(&m)

	for i := 0; i < 10; i++ {
		// push i into pipe
		go func(n int) {
			m.Lock()
			p = append(p, n)
			if len(p) >= 1 {
				// cv.Broadcast()
				cv.Signal()
			}
			m.Unlock()
		}(i)
	}

	for i := 0; i < 10; i++ {
		// lock, block and unlock
		m.Lock()
		for len(p) == 0 && finished == 0 {
			cv.Wait()
		}
		m.Unlock()

		// check if stopped or not empty
		m.Lock()
		if len(p) >= 1 {
			println(p[0])
			p = p[1:]
		}
		m.Unlock()
	}
}
