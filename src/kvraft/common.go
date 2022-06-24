package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type OpType string

type Err string

// Put or Append
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
	return reply
}
