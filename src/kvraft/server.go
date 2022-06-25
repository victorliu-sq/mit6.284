package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const TimeOutDuration = time.Duration(600) * time.Millisecond

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state     map[string]string // key-value
	maxSeqIds map[int64]int     // cid - maxSeqId --> duplicate or not
	OpChans   map[int]chan Op   // logIndex - OpChan
	persister *raft.Persister
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.OpChans = make(map[int]chan Op)
	kv.maxSeqIds = make(map[int64]int)

	// DPrintf("max raft state is %v", maxraftstate)
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.DecodeSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			// Command msg
			// convert Command into Op type
			op := applyMsg.Command.(Op)
			kv.ProcessRaftReply(op)
			index := applyMsg.CommandIndex
			opChan := kv.putIfAbsent(index)
			kv.TrySnapshot(applyMsg.CommandIndex)
			// Try to snapshot
			kv.sendOp(opChan, op)
		} else {
			// Snapshot msg
			kv.DecodeSnapshot(applyMsg.Snapshot)
		}
	}
}

func (kv *KVServer) waitOp(opChan chan Op) Op {
	select {
	case op := <-opChan:
		return op
	case <-time.After(TimeOutDuration):
		return Op{}
	}
}

func (kv *KVServer) sendOp(opChan chan Op, op Op) {
	select {
	case <-opChan:
	default:
	}
	opChan <- op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := kv.newGetOp(*args)
	// index, term
	// set incorrect leader at first
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	// DPrintf("[kv %d] Request [GET]", kv.me)
	// wait for Op from Opchan of [index]
	opChan := kv.putIfAbsent(index)
	newOp := kv.waitOp(opChan)
	// kv.ProcessReply(newOp)
	// DPrintf("[kv %d] with Request [GET] return the value", kv.me)
	if isOpEqual(op, newOp) {
		kv.mu.Lock()
		reply.Value = kv.state[op.Key]
		kv.mu.Unlock()
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := kv.newPutAppendOp(*args)
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("[kv %d] receives Request [%v]", kv.me, op)
	if !isLeader {
		return
	}
	// DPrintf("[kv %d] Request [%v]", kv.me, op)
	opChan := kv.putIfAbsent(index)
	newOp := kv.waitOp(opChan)
	// kv.ProcessReply(newOp)
	// DPrintf("[kv %d] with Request [%v] has updated its state", kv.me, op)
	if isOpEqual(op, newOp) {
		reply.Err = OK
		// DPrintf("[kv %d] key %v 's value becomes %v", kv.me, op.Key, op.Value)
	}
}

func (kv *KVServer) ProcessRaftReply(op Op) {
	// if seqId of op <= maxSeqId of client
	// duplicate op --> ignore

	// if seqId of op > maxSeqId of client
	// (1) update maxSeqId of client
	// (2) if PUT / APPEND --> update kv.state
	kv.mu.Lock()
	defer kv.mu.Unlock()
	maxSeqId, found := kv.maxSeqIds[op.ClientId]
	if !found || op.SequenceId > maxSeqId {
		// not duplicate
		switch op.OpType {
		case PUT:
			kv.state[op.Key] = op.Value
		case APPEND:
			kv.state[op.Key] += op.Value
		}
		// update max seqId at last!!
		kv.maxSeqIds[op.ClientId] = op.SequenceId
	}
}

func (kv *KVServer) putIfAbsent(index int) chan Op {
	// if OpChan of LogIndex has not created
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, found := kv.OpChans[index]; !found {
		kv.OpChans[index] = make(chan Op, 1)
	}
	return kv.OpChans[index]
}

func isOpEqual(op1 Op, op2 Op) bool {
	return op1.OpType == op2.OpType && op1.SequenceId == op2.SequenceId && op1.ClientId == op2.ClientId
}

// ******************************************************************************
// struct

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     OpType
	Key        string
	Value      string
	SequenceId int
	ClientId   int64
}

func (kv *KVServer) newGetOp(args GetArgs) Op {
	GetOp := Op{}
	GetOp.OpType = GET
	GetOp.Key = args.Key
	GetOp.ClientId = args.ClientID
	GetOp.SequenceId = args.SequenceId
	return GetOp
}

func (kv *KVServer) newPutAppendOp(args PutAppendArgs) Op {
	PutAppendOp := Op{}
	PutAppendOp.OpType = OpType(args.Op)
	PutAppendOp.Key = args.Key
	PutAppendOp.Value = args.Value
	PutAppendOp.ClientId = args.ClientId
	PutAppendOp.SequenceId = args.SequenceId
	return PutAppendOp
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
