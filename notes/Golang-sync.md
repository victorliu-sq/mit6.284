# Goroutine

Why we cannot use `a = go func(){...}()`?

This is quite a paradox

goroutine means calling an asynchronous function whereas receiving returned value is synchronous code

```go
// definition
go func(arguments){code...}(values passed in)
```



```go
package main

import "time"

func p(x int) {
	println(x)
}

func main() {
	for i := 0; i < 10; i++ {
		go p(i)
	}
	time.Sleep(1 * time.Second)
}
```

```
func main() {
	for i := 0; i < 10; i++ {
		go func(x int) {
			println(x)
		}(i)
	}
	time.Sleep(1 * time.Second)
}
```



# Waitgroup

For func created inside a func, the local variable can be regarded as locally global. 

We don't have to pass them into func to modify them.

```go
// declaration
var wg sync.WaitGroup
// count + 1
wg.Add(1)
// count - 1
wg.Done()
// wait until count == 0
wg.Wait()
```



```go
// pass by value
package main

import (
	"sync"
)

func main() {
	// declare
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		// counter + 1
		wg.Add(1)
		
		go func(x int) {
			println(x)
			// counter - 1
			wg.Done()
		}(i)
	}
	wg.Wait()
}
```



# Atomic

```go
// definition
var a int64
// add 
atomic.AddInt64(&a, 1)
// load
atomic.LoadInt64(&a)
```



```go
package main

import (
	"sync"
	"sync/atomic"
)

var a int64 = 0

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(x int) {
			atomic.AddInt64(&a, 1)
			println(atomic.LoadInt64(&a))
			wg.Done()
		}(i)
	}
	wg.Wait()
	println(a)
}
```



# Mutex

```go
// definition
var m sync.Mutex

// lock
m.lock()

// unlock
m.unlock()

```



```go
// pass by value
package main

import (
	"sync"
)

func main() {
	var wg sync.WaitGroup
	v := 0
	var m sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(x int) {
			m.Lock()
			v += x
			m.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()

	println(v)
}
```

```go
// pass by address mutex
package main

import (
	"sync"
)

var v int = 0

func main() {
	var wg sync.WaitGroup
	var m sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(x int, wg *sync.WaitGroup, m *sync.Mutex) {
			m.Lock()
			v += x
			m.Unlock()
			wg.Done()
		}(i, &wg, &m)
	}
	wg.Wait()

	println(v)
}
```



# Condition Variable

```go
// definition
var m sync.mutex
cv := sync.NewCond(&m)

// push 
m.lock()
len++
if len >= 1 {
	// cv.BroadCast()   
    cv.Signal()
}
m.unlock()

// pop 
m.lock()
for len == 0 {
    cv.wait()
}
len--
m.unlock()
```

Example

```go
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
			mux.Lock()
			p = append(p, n)
			if len(p) >= 1 {
                cv.Broadcast()
			}
			mux.Unlock()
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
```



# Channel

```go
// definition


```

Examples

```go
// pass by reference

package main

func main() {
	var vs = make(chan int, 2)

	for i := 0; i < 10; i++ {
		// push i into channel
		go func(n int) {
			vs <- n
		}(i)
	}

	for i := 0; i < 10; i++ {
		x := <-vs
		println(x)
	}
}
```

```go
package main

func main() {
	// (chan + type, capacity)
	vs := make(chan int, 2)

	for i := 0; i < 10; i++ {
		// push i into channel
		go func(n int) {
			vs <- n
		}(i)
	}

	count := 0
	for x := range vs {
		println(x)
		count++
		if count == 10 {
			break
		}
	}
}

```



channel + atomic / mutex

channel = synchronization queue = lock + queue --> we cannot lock channel

```go
package main

import (
	"strconv"
	"sync/atomic"
	"time"
)

type ch_s struct {
	ch chan string
}

func add(s *ch_s) {
	for i := 0; i < 5; i++ {
		// push i into channel
		go func(n int) {
			s.ch <- strconv.Itoa(n)
		}(i)
	}
	time.Sleep(1 * time.Second)
	close(s.ch)
}

func main() {
	s := ch_s{}
	s.ch = make(chan string) // channel and map in struct should be initialized

	var count int64

	go add(&s)
	// waiting for 10 strings but at most 5 are added to channel
	// after closing the channel, all remaining strings will become ""
	for x := range s.ch {
		go func(s string) {
			atomic.AddInt64(&count, 1)
			println("x:", s, "count:", atomic.LoadInt64(&count))
		}(x)
	}
	time.Sleep(1 * time.Second)
}
```



# Select

```go
package main

import (
	"fmt"
	"time"
)

func add(c1 chan int, c2 chan int) {
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if i%2 == 0 {
			c1 <- i
		} else {
			c2 <- i
		}
	}
}

func main() {
	c1 := make(chan int)
	c2 := make(chan int)
	ticker := time.NewTicker(500 * time.Millisecond)

	// channel will be passed by reference automatically
	go add(c1, c2)

	go func() {
		for {
			select {
			case n1 := <-c1:
				println("c1", n1)
			case n2 := <-c2:
				println("c2", n2)
			case t := <-ticker.C:
				fmt.Println("Tick at", t)
			}
		}
	}()

	time.Sleep(10 * time.Second)
}

```

