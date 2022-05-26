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

	// Conflict information
	XValid bool // whether conflict
	XTerm  int  // the term conflicting the prevLogTerm
	XIndex int  // index of first entry of Xterm
}

func (rf *Raft) newAEArgs(peer int) AppendEntryArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	// If prev leader's log is very long but most of log entries are not replicated
	// Since nextIndex is optimistic, it can easily go out of range
	next := rf.nextIndex[peer] - 1
	if next <= 0 {
		next = 1
	} else if next > rf.GetLastLogEntry().Index+1 {
		next = rf.GetLastLogEntry().Index
	}

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log[next-1].Term,
		Entries:      make([]LogEntry, rf.GetLastLogEntry().Index-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.GetSubarrayEnd(next))
	// if len(args.Entries) == 0 {
	// 	Debug(dLog, "HeartBeart Msg\n")
	// } else {
	// 	Debug(dLog, "Replication Msg\n")
	// }
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
