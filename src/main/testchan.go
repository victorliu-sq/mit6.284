package main

import (
	"strconv"
	"sync"
	"time"
)

type ch_s struct {
	ch chan string
	m  map[string]int
}

func main() {
	// var vs = make(chan int, 2)
	s := ch_s{}

	s.ch = make(chan string)
	s.m = make(map[string]int)
	s.m["sss"] = 1

	for i := 0; i < 5; i++ {
		// push i into channel
		go func(n int) {
			s.ch <- strconv.Itoa(n)
		}(i)
	}

	var mu sync.Mutex
	count := 0

	for i := 0; i < 10; i++ {
		go func() {
			x := <-s.ch
			if x == "" {
				println("xxx")
			} else {
				mu.Lock()
				defer mu.Unlock()
				count++
				if count == 5 {
					close(s.ch)
				}
				println(x)
			}
		}()
	}
	time.Sleep(1 * time.Second)
}
