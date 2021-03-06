package shardkv

import (
	"fmt"
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
	labgob.Register(GarbageReply{})

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

	kv.config = shardctrler.Config{}
	kv.config.Num = 0
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.shards = make(map[int]bool)
	kv.comeInShards = make(map[int]bool)
	kv.comeInShardsConfigNum = 0
	// kv.comeOutShards2state = make(map[int]map[string]string)
	kv.comeOutShards2state = make(map[int]map[int]map[string]string)

	kv.garbageShards = make(map[int]int)
	kv.garbageShardsConfigNum = 0

	DPrintf("[KV%v] restores", kv.me)
	kv.DecodeSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	go kv.DaemonTryPullConfig()
	go kv.DaemonTryPullShard()
	go kv.DaemonTryGarbageCollection()
	return kv
}

// **********************************************************************************************************
// Pull Config()

func (kv *ShardKV) DaemonTryPullConfig() {
	for !kv.killed() {
		// return if not leader
		kv.TryPullConfig()
		time.Sleep(TimePullConfig)
	}
}

func (kv *ShardKV) TryPullConfig() {
	// return if (1) if not leader (2) last TryPullShard has not finished yet
	if !kv.rf.IsLeaderLock() {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(kv.comeInShards) > 0 {
		return
	}
	nextConfigNum := kv.config.Num + 1
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
	if newConfig.Num <= kv.config.Num {
		return
	}
	oldConfig := kv.config
	comeOutShards := kv.shards
	kv.shards, kv.config = make(map[int]bool), newConfig

	group2shards := shardctrler.GetGroup2Shards(&newConfig)
	// DPrintf("{New Config} from [KV%v] {Group%v}", kv.me, kv.gid)
	// for group, shards := range group2shards {
	// DPrintf("Group: %v, Shards:%v", group, shards)
	// }

	kv.comeInShardsConfigNum = oldConfig.Num
	for _, shard := range group2shards[kv.gid] {
		if _, ok := comeOutShards[shard]; ok || oldConfig.Num == 0 {
			// if first config new shard already in kv
			//  -> stay in kv and not comeOut
			kv.shards[shard] = true
			delete(comeOutShards, shard)
		} else {
			// if new shard not in kv yet
			kv.comeInShards[shard] = true
		}
	}

	kv.comeOutShards2state[oldConfig.Num] = make(map[int]map[string]string)
	for comeOutShard, _ := range comeOutShards {
		kv.comeOutShards2state[oldConfig.Num][comeOutShard] = make(map[string]string)
		outState := make(map[string]string)
		for k, v := range kv.state {
			shard := key2shard(k)
			if shard == comeOutShard {
				// kv.comeOutShards2state[oldConfig.Num][comeOutShard][k] = v
				outState[k] = v
				delete(kv.state, k)
				// DPrintf("[KV%v] deletes state whose key is %v", kv.me, k)
			}
		}
		kv.comeOutShards2state[oldConfig.Num][comeOutShard] = outState
	}

	// DPrintf("[KV%v] {Pull Config} shards : %v", kv.me, kv.GetShardArray())
}

// **********************************************************************************************************
// Pull Shard()
func (kv *ShardKV) DaemonTryPullShard() {
	for !kv.killed() {
		kv.TryPullShard()
		time.Sleep(TimePullShard)
	}
}

func (kv *ShardKV) TryPullShard() {
	// return if (1) if not leader (2) last TryPullConfig has not finished yet
	if !kv.rf.IsLeaderLock() {
		return
	}
	kv.mu.Lock()
	if len(kv.comeInShards) == 0 {
		kv.mu.Unlock()
		return
	}
	// DPrintf("[KV%v] starts to pull shards, config num is %v", kv.me, kv.config.Num)
	oldConfig := kv.mck.Query(kv.config.Num - 1)
	shard2group := oldConfig.Shards
	group2servers := oldConfig.Groups

	var wg sync.WaitGroup
	for comeInShard, _ := range kv.comeInShards {
		wg.Add(1)
		go func(shard int) {
			// DPrintf("[KV%v] starts to pull {Shard%v}", kv.me, shard)
			gid := shard2group[shard]
			servers := group2servers[gid]
			args := kv.newMigrateArgs(shard, kv.comeInShardsConfigNum)
			reply := kv.newMigrateReply(shard, kv.comeInShardsConfigNum)
			for _, server := range servers {
				srcServer := kv.make_end(server)
				ok := srcServer.Call("ShardKV.ShardMigration", &args, &reply)
				// DPrintf("[KV%v] tries to pull {Shard%v} from [Group%v]", kv.me, shard, gid)
				// DPrintf("ok:%v, reply.Err:%v", ok, reply.Err)
				if ok && reply.Err == OK {
					kv.rf.Start(reply)
					// DPrintf("[KV%v] stubs one migrate reply for {Shard%v}", kv.me, args.Shard)
					break
				} else if ok && reply.Err == ErrWrongGroup {
					// DPrintf("[KV%v]{Wrong Group} fails to stub one migrate reply for {Shard%v}", kv.me, args.Shard)
					break
				}
			}
			wg.Done()
		}(comeInShard)
	}
	// DPrintf("[KV%v] waits for migrate replies to finish", kv.me)
	// we need to unlock here
	kv.mu.Unlock()
	wg.Wait()
	// DPrintf("[KV%v] has tried to stub all migrate replies", kv.me)
}

func (kv *ShardKV) ProcessPullShardReply(reply MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongLeader
	if reply.ConfigNum != kv.config.Num-1 {
		return
	}
	shard := reply.Shard
	// DPrintf("[KV%v] tries to process pull shard{Shard%v} reply", kv.me, shard)
	// delete(kv.comeOutShards2state, shard)
	if _, ok := kv.shards[shard]; !ok {
		// state
		for k, v := range reply.State {
			kv.state[k] = v
		}
		// maxSeqId
		for cId, seqId := range reply.MaxSeqIds {
			kv.maxSeqIds[cId] = max(kv.maxSeqIds[cId], seqId)
		}
		kv.shards[shard] = true
		// DPrintf("[Group%v] successfully updates State and MaxSeqId", kv.gid)
		// DPrintf("[KV%v] {Pull Shards} shards : %v (new Shard is %v)", kv.me, kv.GetShardArray(), shard)
	}
	// kv.garbageShardsConfigNum = reply.ConfigNum
	// comeInShardsConfigNum := kv.comeInShards[shard]
	kv.garbageShards[shard] = reply.ConfigNum
	// DPrintf("new garbage configNum is %v", reply.ConfigNum)
	delete(kv.comeInShards, shard)
}

// **********************************************************************************************************
// Migrate RPC
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	if !kv.rf.IsLeaderLock() {
		// DPrintf("Wrong Leader")
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	if args.ConfigNum >= kv.config.Num {
		// DPrintf("Wrong ConfigNum, args ConfigNum:%v, kvMaxConfigNum:%v", args.ConfigNum, kv.config.Num)
		return
	}
	// DPrintf("Correct ConfigNum")
	// if len(kv.comeOutShards2state[args.Shard]) == 0 {
	// 	reply.Err = ErrWrongGroup
	// 	// DPrintf("Wrong Group")
	// 	return
	// }
	// DPrintf("Correct Group")
	reply.State = kv.CopyState(args.ConfigNum, args.Shard)
	reply.MaxSeqIds = kv.CopyMaxSeqId()
	reply.Err = OK

	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	// DPrintf("Pull shard successfully!")
}

func (kv *ShardKV) CopyState(configNum int, shard int) map[string]string {
	state := make(map[string]string)
	for k, v := range kv.comeOutShards2state[configNum][shard] {
		state[k] = v
	}
	return state
}

func (kv *ShardKV) CopyMaxSeqId() map[int64]int {
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
		if applyMsg.SnapshotValid {
			// Snapshot msg
			kv.DecodeSnapshot(applyMsg.Snapshot)
			continue
		}
		if newConfig, ok := applyMsg.Command.(shardctrler.Config); ok {
			kv.ProcessPullConfigReply(newConfig)
			// DPrintf("[KV%v] Snapshots when Pull Config", kv.me)
			kv.TrySnapshot(applyMsg.CommandIndex)
		} else if migrateReply, ok := applyMsg.Command.(MigrateReply); ok {
			// DPrintf("Process got it")
			kv.ProcessPullShardReply(migrateReply)
			// DPrintf("[KV%v] Snapshots when Pull Shard", kv.me)
			kv.TrySnapshot(applyMsg.CommandIndex)
		} else if garbageReply, ok := applyMsg.Command.(GarbageReply); ok {
			kv.ProcessGarbageCollectionReply(garbageReply)
			// DPrintf("[KV%v] Snapshots when Garbage Collect", kv.me)
			kv.TrySnapshot(applyMsg.CommandIndex)
		} else if applyMsg.CommandValid {
			// Command msg
			// convert Command into Op type
			op := applyMsg.Command.(Op)
			kv.ProcessOpReply(&op)
			index := applyMsg.CommandIndex
			opChan := kv.putIfAbsent(index)
			// DPrintf("[KV%v] Snapshots when Put/Append/Get", kv.me)
			kv.TrySnapshot(applyMsg.CommandIndex)
			// Try to snapshot
			kv.sendOp(opChan, op)
		}
	}
}

func (kv *ShardKV) ProcessOpReply(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("[KV%v] {%v} key:%v val: %v has arrived based on val:%v", kv.me, op.OpType, op.Key, op.Value, kv.state[op.Key])
	maxSeqId, found := kv.maxSeqIds[op.CId]
	_, isInComeInShards := kv.comeInShards[key2shard(op.Key)]
	if !kv.matchShardUnlock(op.Key) || isInComeInShards {
		// DPrintf("[KV%v] {%v} not match shard", kv.me, op.OpType)
		op.OpType = ErrWrongGroup
		return
	}

	if op.OpType == GET || !found || op.SeqId > maxSeqId {
		// not duplicate
		switch op.OpType {
		case PUT:
			kv.state[op.Key] = op.Value
		case APPEND:
			kv.state[op.Key] += op.Value
		case GET:
			op.Value = kv.state[op.Key]
		}
		// DPrintf("[KV%v] {%v} now has key-vaule : %v- %v", kv.me, op.OpType, op.Key, kv.state[op.Key])
		// update max seqId at last!!
		kv.maxSeqIds[op.CId] = op.SeqId
	} else {
		// DPrintf("[KV%v] {%v} duplicated", kv.me, op.OpType)
	}
}

// **********************************************************************************************************
// Garbage Collection
func (kv *ShardKV) DaemonTryGarbageCollection() {
	for !kv.killed() {
		kv.TryGarbageCollection()
		time.Sleep(TimePullShard)
	}
}

func (kv *ShardKV) TryGarbageCollection() {
	// return if (1) if not leader (2) last TryPullConfig has not finished yet
	if !kv.rf.IsLeaderLock() {
		return
	}
	kv.mu.Lock()
	if len(kv.garbageShards) == 0 {
		kv.mu.Unlock()
		return
	}
	// DPrintf("[KV%v] starts to delete garbage shards, config num is %v", kv.me, kv.config.Num)

	var wg sync.WaitGroup
	for garbageShard, garbageShardsConfigNum := range kv.garbageShards {
		wg.Add(1)
		oldConfig := kv.mck.Query(garbageShardsConfigNum)
		go func(shard int, config shardctrler.Config) {
			shard2group := config.Shards
			group2servers := config.Groups
			// DPrintf("[KV%v] starts to dekete garbage shard {Shard%v}", kv.me, shard)
			gid := shard2group[shard]
			servers := group2servers[gid]
			args := kv.newGarbageArgs(shard, config.Num)
			reply := kv.newGarbageReply(shard, config.Num)
			for _, server := range servers {
				srcServer := kv.make_end(server)
				ok := srcServer.Call("ShardKV.GarbageCollection", &args, &reply)
				// DPrintf("[KV%v] tries to delete garbage shard {Shard%v} from [Group%v]", kv.me, shard, gid)
				// DPrintf("ok:%v, reply.Err:%v", ok, reply.Err)
				if ok && reply.Err == OK {
					kv.mu.Lock()
					delete(kv.garbageShards, shard)
					kv.mu.Unlock()
					// DPrintf("[KV%v] stubs one garbage reply for {Shard%v}", kv.me, args.Shard)
					break
				} else if ok && reply.Err == ErrWrongGroup {
					// DPrintf("[KV%v]{Wrong Group} fails to stub one migrate reply for {Shard%v}", kv.me, args.Shard)
					break
				}
			}
			wg.Done()
		}(garbageShard, oldConfig)
	}
	// DPrintf("[KV%v] waits for migrate replies to finish", kv.me)
	// we need to unlock here
	kv.mu.Unlock()
	wg.Wait()
	// DPrintf("[KV%v] has tried to stub all migrate replies", kv.me)
}

func (kv *ShardKV) ProcessGarbageCollectionReply(reply GarbageReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// reply.Err = ErrWrongLeader
	// if reply.ConfigNum != kv.config.Num-1 {
	// 	return
	// }
	// delete comeOutShards
	if _, ok := kv.comeOutShards2state[reply.ConfigNum]; ok {
		delete(kv.comeOutShards2state[reply.ConfigNum], reply.Shard)
		DPrintf("[KV%v] delete comeOutShard %v", kv.me, reply.Shard)
		if len(kv.comeOutShards2state[reply.ConfigNum]) == 0 {
			delete(kv.comeOutShards2state, reply.ConfigNum)
			DPrintf("[KV%v] all comeOutShards of configNum %v have been deleted", kv.me, reply.ConfigNum)
		}
	} else {
		DPrintf("[KV%v]{GC} given configNum: %v || kv configNum: %v", kv.me, reply.ConfigNum, kv.config.Num)
	}
	total := 0
	for configNum, _ := range kv.comeOutShards2state {
		total += len(kv.comeOutShards2state[configNum])
	}
	DPrintf("[KV%v] remaining comeOutShards : %v", kv.me, total)
}

// **********************************************************************************************************
// Garbage Collection RPC
func (kv *ShardKV) GarbageCollection(args *GarbageArgs, reply *GarbageReply) {
	if !kv.rf.IsLeaderLock() {
		// DPrintf("Wrong Leader")
		return
	}
	// DPrintf("new garbage configNum is %v", args.ConfigNum)
	kv.mu.Lock()
	// defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if _, ok := kv.comeOutShards2state[args.ConfigNum]; !ok {
		// DPrintf("Wrong ConfigNum, args ConfigNum:%v, kvMaxConfigNum:%v", args.ConfigNum, kv.config.Num)
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.comeOutShards2state[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		return
	}
	// reply.ConfigNum = args.ConfigNum
	// reply.Shard = args.Shard
	kv.mu.Unlock()

	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Err = OK
	kv.rf.Start(*reply)
	// DPrintf("Pull shard successfully!")
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

	config        shardctrler.Config
	shards        map[int]bool
	comeInShards  map[int]bool
	garbageShards map[int]int
	// comeOutShards       map[int]bool
	// comeOutShards2state map[int]map[string]string

	comeInShardsConfigNum  int
	garbageShardsConfigNum int
	comeOutShards2state    map[int]map[int]map[string]string

	mck *shardctrler.Clerk
}

const TimeOutDuration = time.Duration(1000) * time.Millisecond

const TimePullConfig = time.Duration(50) * time.Millisecond

const TimePullShard = time.Duration(25) * time.Millisecond

const TimeGarbageCollection = time.Duration(100) * time.Millisecond

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

func (kv *ShardKV) GetShardArray() string {
	shards := []int{}
	for shard, _ := range kv.shards {
		shards = append(shards, shard)
	}
	return fmt.Sprint(shards)
}

type GarbageArgs struct {
	Shard     int
	ConfigNum int
}

type GarbageReply struct {
	Err
	// for Process Reply
	Shard     int
	ConfigNum int
}

func (kv *ShardKV) newGarbageArgs(shard int, configNum int) GarbageArgs {
	args := GarbageArgs{}
	args.Shard = shard
	args.ConfigNum = configNum
	return args
}

func (kv *ShardKV) newGarbageReply(shard int, configNum int) GarbageReply {
	reply := GarbageReply{}
	reply.Err = ErrWrongLeader
	reply.Shard = shard
	reply.ConfigNum = configNum
	return reply
}
