package shardkv

import (
	"sync/atomic"
	"time"
)

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	// DPrintf("[KV%v] tries to stub one {Get} Op", kv.me)
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		// DPrintf("[KV%v] fails to stub one {Get} Op", kv.me)
		return
	}
	op := kv.newGetOp(*args)
	// index, term
	// set incorrect leader at first
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("[KV%v] stubs one {GET} Op", kv.me)
	if !isLeader {
		return
	}
	opChan := kv.putIfAbsent(index)
	newOp := kv.waitOp(opChan)
	if isOpEqual(op, newOp) {
		kv.mu.Lock()
		reply.Value = kv.state[op.Key]
		kv.mu.Unlock()
		reply.Err = OK
	}
	if newOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
		reply.Value = ""
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	// DPrintf("[KV%v] tries to stub one {Put} Op", kv.me)
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		// DPrintf("[KV%v] fails to stub one {Put} Op", kv.me)
		return
	}
	op := kv.newPutAppendOp(*args)
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("[KV%v] stubs one {Put} Op", kv.me)
	if !isLeader {
		return
	}
	opChan := kv.putIfAbsent(index)
	newOp := kv.waitOp(opChan)
	if isOpEqual(op, newOp) {
		reply.Err = OK
	}
	if newOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) putIfAbsent(index int) chan Op {
	// if OpChan of LogIndex has not created
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, found := kv.OpChans[index]; !found {
		kv.OpChans[index] = make(chan Op, 1)
	}
	return kv.OpChans[index]
}

// *********************************************************************************
// struct and helper function

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpType
	Key    string
	Value  string
	SeqId  int
	CId    int64
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type OpType string

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CId      int64
	LeaderId int
	SeqId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqId int
	CId   int64
}

type GetReply struct {
	Err   Err
	Value string
}

func isOpEqual(op1 Op, op2 Op) bool {
	return op1.OpType == op2.OpType && op1.SeqId == op2.SeqId && op1.CId == op2.CId
}

func (kv *ShardKV) waitOp(opChan chan Op) Op {
	select {
	case op := <-opChan:
		return op
	case <-time.After(TimeOutDuration):
		return Op{}
	}
}

func (kv *ShardKV) sendOp(opChan chan Op, op Op) {
	select {
	case <-opChan:
	default:
	}
	opChan <- op
}

func (kv *ShardKV) newGetOp(args GetArgs) Op {
	GetOp := Op{}
	GetOp.OpType = GET
	GetOp.Key = args.Key
	GetOp.CId = args.CId
	GetOp.SeqId = args.SeqId
	return GetOp
}

func (kv *ShardKV) newPutAppendOp(args PutAppendArgs) Op {
	PutAppendOp := Op{}
	PutAppendOp.OpType = OpType(args.Op)
	PutAppendOp.Key = args.Key
	PutAppendOp.Value = args.Value
	PutAppendOp.CId = args.CId
	PutAppendOp.SeqId = args.SeqId
	return PutAppendOp
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
