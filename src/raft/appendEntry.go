package raft

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Success bool

	// Rejection information
	XTerm  int
	XIndex int
}

func (rf *Raft) newAEArgs(peer int, isHeartBeat bool) AppendEntryArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
		Entries:      make([]LogEntry, 1),
		LeaderCommit: rf.commitIndex,
	}
	if !isHeartBeat {

	}
	return args
}

func (rf *Raft) newAEReply() AppendEntryReply {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return AppendEntryReply{}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// **************************************************************************
// AppendEntry Sender(Leader)

func (rf *Raft) AppendEntryLeader(peer int, isHeartBeat bool) {
	args := rf.newAEArgs(peer, isHeartBeat)
	reply := rf.newAEReply()
	rf.sendAppendEntry(peer, &args, &reply)
}

// AppendEntry Receiver
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {

	// HeartBeat Msg
	// ****************************************************************
	term, isLeader := rf.GetState()
	if args.Term > term || isLeader {
		rf.ConvertToFollower(args.Term)
		if isLeader {
			Debug(dTerm, "[S%d] becomes {Follower}", rf.me, rf.currentTerm)
		}
		if args.Term != term {
			Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		}
	}
	rf.ResetElectionTimer()
	// ****************************************************************
}
