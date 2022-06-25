package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const OpTime = 50 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	leaderId   int
	sequenceId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.sequenceId = 0
	// DPrintf("[ck %d] Hello!", ck.clientId)
	return ck
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := ck.newGetArgs(key)
	reply := ck.newGetReply()
	// DPrintf("[ck %d] Request [GET]", ck.clientId)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if reply.Err == Retry {
			// not leader or not send successfully
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			// not send successfully or time out --> retry
			time.Sleep(OpTime)
		} else if ok && reply.Err == OK {
			return reply.Value
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceId++
	args := ck.newPutAppendArgs(key, value, op, ck.sequenceId)
	reply := ck.newPutAppendReply()
	// DPrintf("[ck %d] Request [%v]", ck.clientId, op)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == Retry {
			// not leader or not send successfully
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			// time out --> retry
			// DPrintf("[ck %d] tries to send Request again", ck.clientId)
			time.Sleep(OpTime)
		} else if ok && reply.Err == OK {
			return
		}
	}
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

// ******************************************************************************
// struct

const (
	OK    = "OK"
	Retry = "Retry"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type OpType string

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId   int64
	LeaderId   int
	SequenceId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SequenceId int
	ClientID   int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (ck *Clerk) newGetArgs(key string) GetArgs {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.clientId
	return args
}

func (ck *Clerk) newGetReply() GetReply {
	reply := GetReply{}
	reply.Err = Retry
	return reply
}

func (ck *Clerk) newPutAppendArgs(key string, value string, op string, sequenceId int) PutAppendArgs {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ClientId = ck.clientId
	args.SequenceId = sequenceId
	args.Op = op
	return args
}

func (ck *Clerk) newPutAppendReply() PutAppendReply {
	reply := PutAppendReply{}
	reply.Err = Retry
	return reply
}
