package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const OpTime = 150 * time.Millisecond

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
	DPrintf("[ck %d] Hello!", ck.clientId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := ck.newGetArgs(key)
	reply := ck.newGetReply()
	DPrintf("[ck %d] Request [GET]", ck.clientId)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || (ok && reply.Err == ErrWrongLeader) {
			// not leader or not send successfully
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else if ok && reply.Err == ErrNoKey {
			// no key
			return ""
		} else if ok && reply.Err == OK {
			// find key
			return reply.Value
		}
		// not send successfully or time out --> retry
		time.Sleep(OpTime)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceId++
	args := ck.newPutAppendArgs(key, value, op, ck.sequenceId)
	reply := ck.newPutAppendReply()
	DPrintf("[ck %d] Request [%v]", ck.clientId, op)
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || (ok && reply.Err == ErrWrongLeader) {
			// not leader or not send successfully
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			// DPrintf("[ck %d] {kv%v} is not leader", ck.clientId, ck.leaderId)
		} else if ok {
			// DPrintf("[ck %d] has updated state with Request [%v]", ck.clientId, op)
			// put / append successfully
			return
		}
		// time out --> retry
		DPrintf("[ck %d] tries to send Request again", ck.clientId)
		time.Sleep(OpTime)
	}
}

// ******************************************************************************
// struct

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
