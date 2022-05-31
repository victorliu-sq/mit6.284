package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logStart || index >= rf.logStart+len(rf.logs) {
		return
	}
	Debug(dSnap, "[S%v] Snapshots state to index %v", rf.me, index)
	// Debug(dSnap, "[S%v]'s logStart is %v, length of logs is %v, index try to cut off is %v", rf.me, rf.logStart, len(rf.logs), index)

	numTrimmed := (index - 1) - rf.GetFirstIndex() + 1
	// Debug(dSnap, "[S%v]'s lastIncludedIndex is  %v", rf.me, rf.lastIncludedIndex)
	// Debug(dSnap, "numTrimmed is %v", numTrimmed)
	// rf.SetLastIncludedIndex(index - 1)
	// rf.SetLastIncludedTerm(rf.GetLogEntry(index - 1).Term)
	rf.CutStart(numTrimmed)
	Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
	Debug(dLog, "[S%d] start becomes: %v", rf.me, rf.GetFirstIndex())
	rf.persistStateAndSnapshot(snapshot)
}

// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	Debug(dSnap, "[S%v] Snapshots state", rf.me)
// 	// Debug(dSnap, "[S%v]'s logStart is %v, length of logs is %v, index try to cut off is %v", rf.me, rf.logStart, len(rf.logs), index)
// 	numTrimmed := index - rf.logStart
// 	// Debug(dSnap, "numTrimmed is %v", numTrimmed)
// 	if numTrimmed > 0 && numTrimmed <= len(rf.logs) {
// 		// rf.SetLastIncludedIndex(index)
// 		// rf.SetLastIncludedTerm(rf.GetLogEntry(index).Term)
// 		rf.CutStart(numTrimmed)
// 	}
// 	rf.persistStateAndSnapshot(snapshot)
// }

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

func (rf *Raft) InstallSnapshotSender(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.sendInstallSnapshot(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all server rule
	if reply.Term > args.Term {
		rf.ConvertToFollower(reply.Term)
	}

	// update nextIndex and matchIndex
	// Debug(dSnap, "[S%v] advance nextIndex of [S%v] to %v", rf.me, peer, rf.lastIncludedIndex+1+len(rf.logs))
	rf.SetNextIndex(peer, args.StartLogIndex+1)
	rf.SetMatchIndex(peer, args.StartLogIndex)
}

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

	// if args.StartLogIndex <= rf.commitIndex {
	// 	return
	// }

	Debug(dLog, "[S%v]{Follower}: startLogIndex is %v, startLogTerm is %v", rf.me, args.StartLogIndex, args.StartLogTerm)
	if rf.IsLogExist(args.StartLogIndex, args.StartLogTerm) {
		// 6. if existing log entry has same index and term as snapshot's last included entry,
		// retain log entry following it and reply
		numTrimmed := args.StartLogIndex - rf.logStart
		rf.CutStart(numTrimmed)
	} else {
		// 7. Discard the entire log and add startLog
		rf.DiscardEntireLog(args.StartLogIndex, args.StartLogTerm)
	}

	Debug(dLog, "[S%v]{Follower} index is %v", rf.me, args.StartLogIndex)
	Debug(dLog, "[S%d]{Follower} log(Term) becomes: %q", rf.me, rf.GetTermArray())
	Debug(dLog, "[S%d]{Follower} start becomes: %v", rf.me, rf.GetFirstIndex())

	// Advance commitIndex and lastAppliedIndex
	rf.commitIndex = args.StartLogIndex
	rf.lastApplied = args.StartLogIndex

	// 2. Create new snapshot file
	// 3. Write data into snapshot file
	// 4.
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.persistStateAndSnapshot(args.Data)

	// 8. Reset state machine using snapshot contents(and load snapshot's cluster configuration)
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.StartLogTerm,
		SnapshotIndex: args.StartLogIndex,
	}
	rf.mu.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		Debug(dError, "{Error} During reading Persis")
	} else {
		// Debug(dPersist, "[S%v] Read Persist successfully", rf.me)
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
		// Debug(dLog, "[S%d] log(Command) becomes: %q", rf.me, rf.GetCommandArray())
	}
}
