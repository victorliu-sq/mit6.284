package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	// cond sync.Cond

	map_ch     chan string
	map_finish map[string]bool
	nMap       int64

	reduce_ch     chan int
	reduce_finish map[int]bool
	nReduce       int64

	total_map    int64
	total_reduce int64
	done         bool
	// file2mnum map[string]int //filename to mapNum
	file2index map[string]int

	hb_chan chan HeartBeatMsg
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Get(req *GetRequest, resp *GetResponse) error {
	for filename := range c.map_ch {
		// filename := <-c.map_ch
		log.Printf("Get(Map): Filename %v\n", filename)
		resp.Filename = filename
		resp.TaskType = Map
		resp.Index = c.file2index[filename]
		resp.NReduce = int(c.total_reduce)
		resp.NMap = int(c.total_map)

		hb_Msg := HeartBeatMsg{}
		hb_Msg.Filename = filename
		hb_Msg.Index = c.file2index[filename]
		hb_Msg.TaskType = Map

		c.hb_chan <- hb_Msg
		return nil
	}

	for idx := range c.reduce_ch {
		// idx := <-c.reduce_ch
		log.Printf("Get(Reduce): Index %v\n", idx)
		resp.TaskType = Reduce
		resp.Index = idx
		resp.NReduce = int(c.total_reduce)
		resp.NMap = int(c.total_map)

		hb_Msg := HeartBeatMsg{}
		hb_Msg.Index = idx
		hb_Msg.TaskType = Reduce

		c.hb_chan <- hb_Msg
		return nil
	}

	resp.TaskType = Done
	return nil
}

func (c *Coordinator) Put(req *PutRequest, resp *PutResponse) error {
	switch taskType := req.TaskType; taskType {
	case Map:
		c.mu.Lock()
		if c.map_finish[req.Filename] {
			c.mu.Unlock()
			return nil
		}
		c.map_finish[req.Filename] = true
		c.mu.Unlock()

		atomic.AddInt64(&c.nMap, 1)
		cur_nMap := atomic.LoadInt64(&c.nMap)
		log.Printf("Put(Map): Filename %v, nMap %d\n", req.Filename, int(cur_nMap))

		if cur_nMap == c.total_map {
			close(c.map_ch)
			log.Printf("Close: map channel\n")
		}
		return nil
	case Reduce:
		c.mu.Lock()
		if c.reduce_finish[req.Index] {
			c.mu.Unlock()
			return nil
		}
		c.reduce_finish[req.Index] = true
		c.mu.Unlock()

		atomic.AddInt64(&c.nReduce, 1)

		log.Printf("Put(Reduce): Index %d, nReduce %d\n", req.Index, int(atomic.LoadInt64(&c.nReduce)))

		if atomic.LoadInt64(&c.nReduce) == c.total_reduce {
			log.Printf("Close: reduce channel\n")
			c.mu.Lock()
			c.done = true
			c.mu.Unlock()
		}
		return nil
	}
	return nil
}

func (c *Coordinator) Check_HeartBeat() {
	for msg := range c.hb_chan {
		go func(hb_Msg HeartBeatMsg) {
			time.Sleep(30 * time.Second)
			switch taskType := hb_Msg.TaskType; taskType {
			case Map:
				c.mu.Lock()
				finished := c.map_finish[hb_Msg.Filename]
				c.mu.Unlock()
				if !finished {
					log.Printf("Map %v Time out!", hb_Msg.Filename)
					c.map_ch <- hb_Msg.Filename
				}
			case Reduce:
				c.mu.Lock()
				finished := c.reduce_finish[hb_Msg.Index]
				c.mu.Unlock()
				if !finished {
					log.Printf("Reduce %v Time out!", hb_Msg.Index)
					c.reduce_ch <- hb_Msg.Index
				}
			}
		}(msg)
	}
}

// func (c *Coordinator) HeartBeat(req *HeartBeatRequest, resp *HeartBeatResponse) error {
// 	time.Sleep(20 * time.Second)
// 	switch taskType := req.TaskType; taskType {
// 	case Map:
// 		filename := req.Filename
// 		c.mu.Lock()
// 		finished := c.map_finish[filename]
// 		c.mu.Unlock()

// 		if !finished {
// 			log.Printf("Map %v Out of time!\n", filename)
// 			c.map_ch <- filename
// 			resp.KILL = KILL
// 			return nil
// 		}
// 	case Reduce:
// 		index := req.Index
// 		c.mu.Lock()
// 		finished := c.reduce_finish[index]
// 		c.mu.Unlock()

// 		if !finished {
// 			log.Printf("Reduce %v Out of time!\n", index)
// 			c.reduce_ch <- index
// 			resp.KILL = KILL
// 			return nil
// 		}
// 	}

// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // serve all rpcs
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		close(c.hb_chan)
	}
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	// initialize coordinator
	c.total_map = int64(len(files))
	c.map_ch = make(chan string, len(files))
	c.map_finish = make(map[string]bool)
	for i := 0; i < len(files); i++ {
		c.map_ch <- files[i]
		c.map_finish[files[i]] = false
	}

	c.total_reduce = int64(nReduce)
	c.reduce_ch = make(chan int, nReduce)
	c.reduce_finish = make(map[int]bool)
	for i := 1; i <= nReduce; i++ {
		c.reduce_ch <- i
		c.reduce_finish[i] = false
	}
	atomic.StoreInt64(&c.nMap, 0)
	atomic.StoreInt64(&c.nReduce, 0)

	c.done = false

	c.file2index = make(map[string]int)
	for idx, filename := range files {
		c.file2index[filename] = idx
	}

	c.hb_chan = make(chan HeartBeatMsg, len(files)+nReduce)
	go func() {
		c.Check_HeartBeat()
	}()
	c.server()
	return &c
}
