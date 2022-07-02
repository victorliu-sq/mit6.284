package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	atomic.StoreInt32(&kv.dead, 0)
	kv.state = make(map[string]string)
	kv.OpChans = make(map[int]chan Op)
	kv.maxSeqIds = make(map[int64]int)
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.maxConfigNum = 0
	kv.shards = make(map[int]bool)
	kv.comeInShards = make(map[int]bool)
	kv.comeOutShards2state = make(map[int]map[string]string)

	kv.DecodeSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	go kv.DaemonTryPullConfig()
	go kv.DaemonTryPullShard()
	return kv
}

// **********************************************************************************************************
// Pull Config()

func (kv *ShardKV) DaemonTryPullConfig() {
	for !kv.killed() {
		// return if not leader
		kv.mu.Lock()
		kv.TryPullConfig()
		kv.mu.Unlock()
		time.Sleep(TimePullConfig)
	}
}

func (kv *ShardKV) TryPullConfig() {
	// return if (1) if not leader (2) last TryPullShard has not finished yet
	if !kv.rf.IsLeaderLock() || len(kv.comeInShards) > 0 {
		return
	}
	nextConfigNum := kv.maxConfigNum + 1
	newConfig := kv.mck.Query(nextConfigNum)
	if nextConfigNum == newConfig.Num {
		kv.rf.Start(newConfig)
	}
}

func (kv *ShardKV) ProcessPullConfigReply(newConfig shardctrler.Config) {
	// update comeInShards, comeOutShards, config, state[comeInShards]
	// oldConfig := kv.config
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if newConfig.Num <= kv.maxConfigNum {
		return
	}
	newShards := make(map[int]bool)
	comeInShards := make(map[int]bool)
	comeOutShards := kv.shards
	comeOutShards2state := make(map[int]map[string]string)

	group2shards := shardctrler.GetGroup2Shards(&newConfig)
	for _, shard := range group2shards[kv.gid] {
		if _, ok := comeOutShards[shard]; ok || kv.maxConfigNum == 0 {
			// if first config new shard already in kv
			//  -> stay in kv and not comeOut
			newShards[shard] = true
			delete(comeOutShards, shard)
		} else {
			// if new shard not in kv yet
			comeInShards[shard] = true
		}
	}

	for comeOutShard, _ := range comeOutShards {
		comeOutShards2state[comeOutShard] = make(map[string]string)
		for k, v := range kv.state {
			shard := key2shard(k)
			if shard == comeOutShard {
				comeOutShards2state[comeOutShard][k] = v
				delete(kv.state, k)
			}
		}
	}

	kv.config = newConfig
	kv.shards = newShards
	kv.comeInShards = comeInShards
	kv.comeOutShards2state = comeOutShards2state
	// kv.comeOutShards = comeOutShards
	kv.maxConfigNum = newConfig.Num
	DPrintf("[Group%v] Pull Config successfully!", kv.gid)
}

// **********************************************************************************************************
// Pull Shard()
func (kv *ShardKV) DaemonTryPullShard() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.TryPullShard()
		kv.mu.Unlock()
		time.Sleep(TimePullShard)
	}
}

func (kv *ShardKV) TryPullShard() {
	// return if (1) if not leader (2) last TryPullConfig has not finished yet
	if !kv.rf.IsLeaderLock() || len(kv.comeInShards) == 0 {
		return
	}
	DPrintf("[Group%v] starts to pull shards, config num is %v", kv.gid, kv.config.Num)
	oldConfig := kv.mck.Query(kv.maxConfigNum - 1)
	shard2group := oldConfig.Shards
	group2servers := oldConfig.Groups

	var wg sync.WaitGroup
	for comeInShard, _ := range kv.comeInShards {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			DPrintf("[Group%v] starts to pull {Shard%v}", kv.gid, shard)
			gid := shard2group[shard]
			servers := group2servers[gid]
			args := kv.newMigrateArgs(shard, kv.maxConfigNum-1)
			reply := kv.newMigrateReply(shard, kv.maxConfigNum-1)
			for _, server := range servers {
				srcServer := kv.make_end(server)
				ok := srcServer.Call("ShardKV.ShardMigration", &args, &reply)
				DPrintf("[Group%v] tries to pull {Shard%v} from [Group%v]", kv.gid, shard, gid)
				// DPrintf("ok:%v, reply.Err:%v", ok, reply.Err)
				if ok && reply.Err == OK {
					kv.rf.Start(reply)
					DPrintf("[Group%v] stubs one migrate reply for {Shard%v}", kv.gid, args.Shard)
					break
				} else if ok && reply.Err == ErrWrongGroup {
					DPrintf("[Group%v]{Wrong Group} fails to stub one migrate reply for {Shard%v}", kv.gid, args.Shard)
					break
				} else {
					if !ok {
						DPrintf("[Group%v]{Fail to Send} fails to stub one migrate reply for {Shard%v}", kv.gid, args.Shard)
					} else {
						DPrintf("[Group%v]{Wrong Leader} fails to stub one migrate reply for {Shard%v}", kv.gid, args.Shard)
					}
				}
			}
		}(comeInShard)
	}
	wg.Wait()
	DPrintf("[Group%v] has tried to stub all migrate replies", kv.gid)
}

func (kv *ShardKV) ProcessPullShardReply(reply MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if reply.ConfigNum != kv.maxConfigNum-1 {
	// 	return
	// }
	shard := reply.Shard
	DPrintf("[Group%v] tries to process pull shard{Shard%v} reply", kv.gid, shard)
	delete(kv.comeInShards, shard)
	// delete(kv.comeOutShards2state, shard)
	if _, ok := kv.shards[reply.Shard]; !ok {
		// state
		for k, v := range reply.State {
			kv.state[k] = v
		}
		// maxSeqId
		for cId, seqId := range reply.MaxSeqIds {
			kv.maxSeqIds[cId] = max(kv.maxSeqIds[cId], seqId)
		}
		kv.shards[shard] = true
		DPrintf("[Group%v] successfully updates State and MaxSeqId", kv.gid)
	}
}

// **********************************************************************************************************
// Migrate RPC
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.rf.IsLeaderLock() {
		// DPrintf("Wrong Leader")
		return
	}
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.maxConfigNum {
		DPrintf("Wrong ConfigNum, args ConfigNum:%v, kvMaxConfigNum:%v", args.ConfigNum, kv.maxConfigNum)
		return
	}
	// DPrintf("Correct ConfigNum")
	if len(kv.comeOutShards2state[args.Shard]) == 0 {
		reply.Err = ErrWrongGroup
		// DPrintf("Wrong Group")
		return
	}
	// DPrintf("Correct Group")
	reply.State = kv.CopyState(args.Shard)
	reply.MaxSeqIds = kv.CopyMaxSeqId(args.Shard)
	reply.Err = OK
	DPrintf("Pull shard successfully!")
}

func (kv *ShardKV) CopyState(shard int) map[string]string {
	state := make(map[string]string)
	for k, v := range kv.comeOutShards2state[shard] {
		state[k] = v
	}
	return state
}

func (kv *ShardKV) CopyMaxSeqId(shard int) map[int64]int {
	maxSeqId := make(map[int64]int)
	for k, v := range kv.maxSeqIds {
		maxSeqId[k] = v
	}
	return maxSeqId
}

// **********************************************************************************************************
// applier

func (kv *ShardKV) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if newConfig, ok := applyMsg.Command.(shardctrler.Config); ok {
			kv.ProcessPullConfigReply(newConfig)
		} else if migrateReply, ok := applyMsg.Command.(MigrateReply); ok {
			DPrintf("Process got it")
			kv.ProcessPullShardReply(migrateReply)
		} else if applyMsg.CommandValid {
			// Command msg
			// convert Command into Op type
			op := applyMsg.Command.(Op)
			kv.ProcessOpReply(op)
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

func (kv *ShardKV) ProcessOpReply(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("KV%v {%v} has arrived", kv.me, op.OpType)
	maxSeqId, found := kv.maxSeqIds[op.CId]
	if !kv.matchShardUnlock(op.Key) {
		DPrintf("KV%v {%v} not match shard", kv.me, op.OpType)
		op.OpType = ErrWrongGroup
		return
	}

	if !found || op.SeqId > maxSeqId {
		// not duplicate
		switch op.OpType {
		case PUT:
			kv.state[op.Key] = op.Value
		case APPEND:
			kv.state[op.Key] += op.Value
		}
		DPrintf("[KV%v] {%v} has key-vaule : %v- %v", kv.me, op.OpType, op.Key, kv.state[op.Key])
		// update max seqId at last!!
		kv.maxSeqIds[op.CId] = op.SeqId
	}
}

// ************************************************************************
// struct and helper function

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state     map[string]string // key-value
	maxSeqIds map[int64]int     // cid - maxSeqId --> duplicate or not
	OpChans   map[int]chan Op   // logIndex - OpChan
	persister *raft.Persister

	dead int32 // set by Kill()

	config       shardctrler.Config
	maxConfigNum int
	shards       map[int]bool
	comeInShards map[int]bool
	// comeOutShards       map[int]bool
	comeOutShards2state map[int]map[string]string

	mck *shardctrler.Clerk
}

const TimeOutDuration = time.Duration(600) * time.Millisecond

const TimePullConfig = time.Duration(50) * time.Millisecond

const TimePullShard = time.Duration(80) * time.Millisecond

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err
	State     map[string]string
	MaxSeqIds map[int64]int
	// for Process Reply
	Shard     int
	ConfigNum int
}

func (kv *ShardKV) newMigrateArgs(shard int, configNum int) MigrateArgs {
	args := MigrateArgs{}
	args.Shard = shard
	args.ConfigNum = configNum
	return args
}

func (kv *ShardKV) newMigrateReply(shard int, configNum int) MigrateReply {
	reply := MigrateReply{}
	reply.Err = ErrWrongLeader
	reply.Shard = shard
	reply.ConfigNum = configNum
	return reply
}

func (kv *ShardKV) matchShard(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard_id := key2shard(key)
	ok := kv.config.Shards[shard_id] == kv.gid
	return ok
}

func (kv *ShardKV) matchShardUnlock(key string) bool {
	shard_id := key2shard(key)
	ok := kv.config.Shards[shard_id] == kv.gid
	return ok
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
