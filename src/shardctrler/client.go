package shardctrler

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

//
// Shardctrler clerk.
//

const RetryInterval = time.Duration(30) * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	seqId    int
	clientId int64
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
	// Your code here.
	ck.seqId = -1
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				// DPrintf("Hello from Query Client")
				return reply.Config
			}
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	// DPrintf("Hello from Join Client")
	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId

	args.Servers = servers
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	// DPrintf("Hello from Leave Client")
	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId

	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId

	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RetryInterval)
	}
}
