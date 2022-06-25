package raft

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// ****************************************************************************************
// Snapshot
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if index <= rf.logStart || index >= rf.logStart+len(rf.logs) {
	// 	return
	// }
	if index <= rf.logStart {
		return
	}
	// Debug(dSnap, "[S%v] Snapshots state to index %v", rf.me, index)

	// numTrimmed := (index - 1) - rf.GetFirstIndex() + 1
	numTrimmed := index - rf.GetFirstIndex()
	// delete log entries from start to index - 1, rf.start = index
	Debug(dSnap, "[S%v]{Leader}, old start is %v, length of log is %v, new start is %v", rf.me, rf.logStart, len(rf.logs), rf.logStart+numTrimmed)
	rf.CutStart(numTrimmed)
	rf.persistStateAndSnapshot(snapshot)
}

func (rf *Raft) CutStart(numTrimmed int) {
	// cut nums up to index
	rf.logs = append([]LogEntry(nil), rf.logs[numTrimmed:]...)
	rf.logStart += numTrimmed
}

// *****************************************************************************************
// InstallSnapshotSender
func (rf *Raft) InstallSnapshotSender(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.sendInstallSnapshot(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// kv-server no longer leader or leader crashes
	// if rf.state != Leader || rf.currentTerm != args.Term {
	// 	return
	// }

	// all server rule
	if reply.Term > args.Term {
		rf.ConvertToFollower(reply.Term)
		return
	}
	// update nextIndex and matchIndex
	rf.SetNextIndex(peer, args.StartLogIndex+1)
	rf.SetMatchIndex(peer, args.StartLogIndex)
}

// *****************************************************************************************
// InstallSnapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// all server rule
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
	}

	// 1. Reply immediately if term < currentTerm
	reply.Term = rf.GetTerm()
	if args.Term < rf.GetTerm() {
		return
	}

	// check if snapshot has expired
	if args.StartLogIndex <= rf.commitIndex {
		return
	}

	// Debug(dLog, "[S%v]{Follower}: startLogIndex is %v, startLogTerm is %v", rf.me, args.StartLogIndex, args.StartLogTerm)
	if rf.IsLogExist(args.StartLogIndex, args.StartLogTerm) {
		// 6. if existing log entry has same index and term as snapshot's last included entry,
		// retain log entry following it and reply
		numTrimmed := args.StartLogIndex - rf.logStart
		rf.CutStart(numTrimmed)
	} else {
		// 7. Discard the entire log and add startLog
		rf.DiscardEntireLog(args.StartLogIndex, args.StartLogTerm)
	}

	Debug(dSnap, "[S%v]{Leader}, old start is %v, length of log is %v, new start is %v", rf.me, rf.logStart, len(rf.logs), args.StartLogIndex)
	// Debug(dLog, "[S%v]{Follower} index is %v", rf.me, args.StartLogIndex)
	// Debug(dLog, "[S%d]{Follower} log(Term) becomes: %q", rf.me, rf.GetTermArray())
	// Debug(dLog, "[S%d]{Follower} start becomes: %v", rf.me, rf.GetFirstIndex())

	// 2. Create new snapshot file
	// 3. Write data into snapshot file
	// 4.
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index

	// Advance commitIndex and lastAppliedIndex
	rf.commitIndex = max(rf.commitIndex, args.StartLogIndex)
	rf.lastApplied = max(rf.lastApplied, args.StartLogIndex)

	// 8. Reset state machine using snapshot contents(and load snapshot's cluster configuration)

	// To avoid read back in time in kv server
	if rf.lastApplied > args.StartLogIndex {
		return
	}

	rf.persistStateAndSnapshot(args.Data)

	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.StartLogTerm,
		SnapshotIndex: args.StartLogIndex,
	}
	rf.mu.Lock()
}

func (rf *Raft) IsLogExist(startLogIndex int, startLogTerm int) bool {
	if rf.logStart <= startLogIndex && startLogIndex <= rf.logStart+len(rf.logs)-1 && rf.GetLogEntry(startLogIndex).Term == startLogTerm {
		return true
	} else {
		return false
	}
}

func (rf *Raft) DiscardEntireLog(startLogIndex int, startLogTerm int) {
	rf.logs = append([]LogEntry(nil), LogEntry{Command: nil, Term: startLogTerm, Index: startLogIndex})
	rf.logStart = startLogIndex
}

// *****************************************************************************************
type InstallSnapshotArgs struct {
	Term          int
	LeaderId      int
	StartLogIndex int
	StartLogTerm  int
	Data          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) NewInstallSnapshotArgs() InstallSnapshotArgs {
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.StartLogIndex = rf.GetFirstLogEntry().Index
	args.StartLogTerm = rf.GetFirstLogEntry().Term
	args.Data = rf.persister.ReadSnapshot()
	return args
}

func (rf *Raft) NewInstallSnapshotReply() InstallSnapshotReply {
	reply := InstallSnapshotReply{}
	return reply
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
