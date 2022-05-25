package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	get(mapf, reducef)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func get(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var done int64
	atomic.StoreInt64(&done, 0)
	for int(atomic.LoadInt64(&done)) == 0 {
		req := GetRequest{}
		resp := GetResponse{}
		call("Coordinator.Get", &req, &resp)
		switch task := resp.TaskType; task {
		case Map:
			if resp.Filename != "" {
				log.Printf("Get(map): %v done!\n", resp.Filename)
				// go heartBeat(resp.TaskType, resp.Filename, resp.Index, &done)

				// time.Sleep(1 * time.Second)
				err := perform_map(resp.Filename, resp.Index, resp.NReduce, mapf)
				if err != nil {
					log.Fatal("fail to perform map!\n")
				}

				// if int(atomic.LoadInt64(&done)) == 0 {
				// 	put(resp.TaskType, resp.Filename, resp.Index)
				// 	log.Printf("Put(map): %v done!\n", resp.Filename)
				// } else {
				// 	log.Printf("Put(map): %v out of time! Worker has dead\n", resp.Filename)
				// }
				put(resp.TaskType, resp.Filename, resp.Index)
				log.Printf("Put(map): %v done!\n", resp.Filename)
			}
		case Reduce:
			if resp.Index != 0 {
				log.Printf("Get(reduce): %v done!\n", resp.Index)
				// go heartBeat(resp.TaskType, resp.Filename, resp.Index, &done)

				// time.Sleep(1 * time.Second)
				// Notice that we add 1 to resp.Index
				err := perform_reduce(resp.Index-1, resp.NMap, resp.NReduce, reducef)
				if err != nil {
					log.Fatal("fail to perform reduce!\n")
				}

				// if int(atomic.LoadInt64(&done)) == 0 {
				// 	put(resp.TaskType, resp.Filename, resp.Index)
				// 	log.Printf("Put(reduce): %d done!\n", resp.Index)
				// } else {
				// 	log.Printf("Put(reduce): %d out of time! Worker has dead\n", resp.Index)
				// }
				put(resp.TaskType, resp.Filename, resp.Index)
				log.Printf("Put(reduce): %d done!\n", resp.Index)
			}

		case Done:
			log.Printf("work is Over\n")
			atomic.StoreInt64(&done, 1)
		}
	}
}

// func perform_map(mapf func(string, string) []KeyValue, filename string, idx int) {

// }

func put(task_type TaskType, filename string, index int) {
	req := PutRequest{}
	resp := PutResponse{}
	req.TaskType = task_type
	req.Filename = filename
	req.Index = index
	call("Coordinator.Put", &req, &resp)
}

// func heartBeat(task_type TaskType, filename string, index int, done *int64) {
// 	req := HeartBeatRequest{}
// 	resp := HeartBeatResponse{}
// 	req.Filename = filename
// 	req.TaskType = task_type
// 	req.Index = index
// 	call("Coordinator.HeartBeat", &req, &resp)
// 	if resp.KILL == KILL {
// 		atomic.StoreInt64(done, 1)
// 	}
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Perform Map
func perform_map(filename string, index int, nReduce int, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	encs := []*json.Encoder{}
	inter_filenames := []string{}
	tmp_files := []*os.File{}
	tmp_filenames := []string{}

	for i := 0; i < nReduce; i++ {
		tmp_file, err := ioutil.TempFile("/mnt/c/Users/jiaxi/6.824/src/main/", "mr-out-")
		if err != nil {
			log.Fatal("cannot open tmpFile")
		}

		tmp_files = append(tmp_files, tmp_file)
		tmp_filename := tmp_file.Name()
		tmp_filenames = append(tmp_filenames, tmp_filename)
		encs = append(encs, json.NewEncoder(tmp_file))

		inter_filename := "mr" + "-out-" + strconv.Itoa(index) + "-" + strconv.Itoa(i)
		inter_filenames = append(inter_filenames, inter_filename)
	}

	for _, kv := range kva {
		k := kv.Key
		reduceNum := ihash(k) % nReduce
		enc := encs[reduceNum]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write %v", inter_filenames[reduceNum])
		}
	}

	for i := 0; i < nReduce; i++ {
		tmp_files[i].Close()
	}

	for i := 0; i < nReduce; i++ {
		err := os.Rename(tmp_filenames[i], inter_filenames[i])
		if err != nil {
			log.Fatal("cannot rename")
		}
	}
	return nil
}

// Perform Reduce
func perform_reduce(index int, nMap int, nReduce int, reducef func(string, []string) string) error {
	kva := []KeyValue{}

	for i := 0; i < nMap; i++ {
		filename := "mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(index)
		// log.Printf(filename)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("cannot open ", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()

		err = os.Remove(filename)
		if err != nil {
			log.Fatal("fail to delete ", filename)
		}
	}

	sort.Sort(ByKey(kva))

	tmp_file, err := ioutil.TempFile("/mnt/c/Users/jiaxi/6.824/src/main/", "mr-out-")
	if err != nil {
		log.Fatal("cannot open tmpFile")
	}
	tmp_filename := tmp_file.Name()
	file_name := "mr-out-" + strconv.Itoa(index)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp_file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tmp_file.Close()
	os.Rename(tmp_filename, file_name)
	return nil
}
