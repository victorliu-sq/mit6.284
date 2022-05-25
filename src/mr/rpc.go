package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	Map      TaskType = 1
	Reduce   TaskType = 2
	Done     TaskType = 3
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	KILL              = "KIll"
)

type GetRequest struct {
}

type GetResponse struct {
	Filename string
	Index    int
	TaskType TaskType
	NReduce  int
	NMap     int
}

type PutRequest struct {
	Filename string
	Index    int
	TaskType TaskType
}

type PutResponse struct {
}

// type HeartBeatRequest struct {
// 	Filename string
// 	Index    int
// 	TaskType TaskType
// }

// type HeartBeatResponse struct {
// 	KILL string
// }

type HeartBeatMsg struct {
	Filename string
	Index    int
	TaskType TaskType
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
