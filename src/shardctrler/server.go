package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// *************************************************************************************
// PutOp
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[SC%v]{JOIN}: groups: %v", sc.me, GetJoinGroups(args.Servers))
	reply.WrongLeader = true
	op := newOp(*args, JOIN, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	opChan := sc.putIfAbsent(index)
	newOp := sc.waitOp(opChan)
	if isOpEqual(op, newOp) {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("[SC%v]{LEAVE}: groups: %v", sc.me, GetLeaveGroups(args.GIDs))
	reply.WrongLeader = true
	op := newOp(*args, LEAVE, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	opChan := sc.putIfAbsent(index)
	newOp := sc.waitOp(opChan)
	if isOpEqual(op, newOp) {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	op := newOp(*args, MOVE, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	opChan := sc.putIfAbsent(index)
	newOp := sc.waitOp(opChan)
	if isOpEqual(op, newOp) {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

// *************************************************************************************
// GetOp
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// DPrintf("{Query}[SC%v] with Num %v", sc.me, args.Num)
	reply.WrongLeader = true
	op := newOp(*args, QUERY, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	opChan := sc.putIfAbsent(index)
	newOp := sc.waitOp(opChan)

	if isOpEqual(op, newOp) {
		Num := getNum(newOp.Args)
		sc.mu.Lock()
		if Num >= 0 && Num < len(sc.configs) {
			reply.Config = sc.getConfig(Num)
			// DPrintf("Num of Groups: %v", len(reply.Config.Groups))
		} else {
			reply.Config = sc.configs[len(sc.configs)-1]
			// DPrintf("Num of Groups: %v", len(reply.Config.Groups))
		}
		sc.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.OpChans = make(map[int]chan Op)
	sc.maxSeqIds = make(map[int64]int)
	atomic.StoreInt32(&sc.dead, 0)

	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		// DPrintf("Receive applymsg")
		applyMsg := <-sc.applyCh
		if !applyMsg.CommandValid {
			continue
		}
		// DPrintf("[S%v] gets ApplyMsg [%v]", sc.me, applyMsg.Command)
		op := applyMsg.Command.(Op)
		sc.ProcessSCReply(op)
		index := applyMsg.CommandIndex
		OpChan := sc.putIfAbsent(index)
		sc.sendOp(OpChan, op)
	}
}

func (sc *ShardCtrler) ProcessSCReply(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	maxSeqId, ok := sc.maxSeqIds[op.ClientId]
	// check duplicate
	if op.OpType == QUERY {
		return
	}

	// update max SeqId
	if !ok || op.SeqId > maxSeqId {
		sc.maxSeqIds[op.ClientId] = op.SeqId
	}

	lastConfig := sc.copyLastConfig()
	if op.OpType == JOIN {
		// Servers map[int][]string // new GID -> servers mappings
		args := op.Args.(JoinArgs)
		tmp := []int{}
		for gid, _ := range args.Servers {
			tmp = append(tmp, gid)
		}
		sort.Ints(tmp)
		for _, gid := range tmp {
			Balance_Join(&lastConfig, gid, args.Servers[gid])
		}
	} else if op.OpType == MOVE {
		args := op.Args.(MoveArgs)
		lastConfig.Shards[args.Shard] = args.GID
	} else if op.OpType == LEAVE {
		args := op.Args.(LeaveArgs)
		tmp := args.GIDs
		sort.Ints(tmp)
		for _, gid := range tmp {
			Balance_Leave(&lastConfig, gid)
		}
	} else {
		return
	}
	sc.configs = append(sc.configs, lastConfig)
	// sc.Balance()
	DPrintf("[S%v]shards after %v of %v: %v", sc.me, op.OpType, op.SeqId, fmt.Sprint(lastConfig.Shards))
	// DPrintf("Num of Groups: %v", len(sc.configs[len(sc.configs)-1].Groups))
}

func Balance_Join(lastConfig *Config, gid int, servers []string) {
	// add one group to config
	lastConfig.Groups[gid] = servers

	// move avg shards from group with max shards to gid
	group2shards := GetGroup2Shards(lastConfig)
	avg_shards := NShards / len(lastConfig.Groups)
	for i := 0; i < avg_shards; i++ {
		maxGid := GetMaxGid(group2shards)
		shard := group2shards[maxGid][0]
		lastConfig.Shards[shard] = gid
		group2shards[maxGid] = group2shards[maxGid][1:]
	}
}

func Balance_Leave(lastConfig *Config, gid int) {
	// delete the group from config
	delete(lastConfig.Groups, gid)

	// move all shards in gid to groups with min shards
	group2shards := GetGroup2Shards(lastConfig)
	shards := group2shards[gid]

	delete(group2shards, gid)
	if len(lastConfig.Groups) == 0 {
		lastConfig.Shards = [NShards]int{}
		return
	}

	for _, shard := range shards {
		minGid := GetMinGid(group2shards)
		lastConfig.Shards[shard] = minGid
		group2shards[minGid] = append(group2shards[minGid], shard)
	}

}

func GetGroup2Shards(lastConfig *Config) map[int][]int {
	group2shards := make(map[int][]int)
	for gid, _ := range lastConfig.Groups {
		group2shards[gid] = []int{}
	}
	for sid, gid := range lastConfig.Shards {
		group2shards[gid] = append(group2shards[gid], sid)
	}
	for gid, _ := range group2shards {
		sort.Ints(group2shards[gid])
	}
	return group2shards
}

func GetMaxGid(group2shards map[int][]int) int {
	max := -1
	maxGid := -1
	for gid, shards := range group2shards {
		if len(shards) > max || (len(shards) == max && gid < maxGid) {
			max = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}

func GetMinGid(group2shards map[int][]int) int {
	min := 10000
	minGid := -1
	for gid, shards := range group2shards {
		if len(shards) < min || (len(shards) == min && gid < minGid) {
			min = len(shards)
			minGid = gid
		}
	}
	return minGid
}

func GetJoinGroups(group2server map[int][]string) string {
	groups := []int{}
	for gid, _ := range group2server {
		groups = append(groups, gid)
	}
	return fmt.Sprint(groups)
}

func GetLeaveGroups(gids []int) string {
	return fmt.Sprint(gids)
}

// *************************************************************************************
// Helper func
func isOpEqual(op1 Op, op2 Op) bool {
	return op1.OpType == op2.OpType && op1.SeqId == op2.SeqId && op1.ClientId == op2.ClientId
}

func (sc *ShardCtrler) copyLastConfig() Config {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1
	newConfig.Shards = [NShards]int{}
	for shard_id, group_id := range lastConfig.Shards {
		newConfig.Shards[shard_id] = group_id
	}
	// newConfig.Shards = lastConfig.Shards
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append(newConfig.Groups[gid], servers...)
	}
	return newConfig
}

func getNum(args interface{}) int {
	return args.(QueryArgs).Num
}

func (sc *ShardCtrler) getConfig(Num int) Config {
	return sc.configs[Num]
}

func (sc *ShardCtrler) putIfAbsent(index int) OpChan {
	// if OpChan of LogIndex has not created
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, found := sc.OpChans[index]; !found {
		sc.OpChans[index] = make(chan Op, 1)
	}
	return sc.OpChans[index]
}

func (sc *ShardCtrler) waitOp(opChan chan Op) Op {
	select {
	case op := <-opChan:
		return op
	case <-time.After(TimeOutDuration):
		return Op{}
	}
}

func (sc *ShardCtrler) sendOp(opChan chan Op, op Op) {
	select {
	case <-opChan:
	default:
	}
	opChan <- op
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func newOp(args interface{}, opType OpType, clientId int64, seqId int) Op {
	op := Op{}
	op.Args = args
	op.OpType = opType
	op.ClientId = clientId
	op.SeqId = seqId
	return op
}

// *********************************************************************************************
// struct
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead      int32
	maxSeqIds map[int64]int   // cid - maxSeqId --> duplicate or not
	OpChans   map[int]chan Op // logIndex - OpChan

	configs []Config // indexed by config num
}

const (
	JOIN  = "JOIN"
	MOVE  = "MOVE"
	LEAVE = "LEAVE"
	QUERY = "QUERY"
)

type OpType string

type OpChan chan Op

type Op struct {
	// Your data here.
	OpType   OpType
	Args     interface{}
	ClientId int64
	SeqId    int
}

const TimeOutDuration = time.Duration(600) * time.Millisecond
