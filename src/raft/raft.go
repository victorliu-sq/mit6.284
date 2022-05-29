package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile State
	commitIndex int
	lastApplied int

	// DIY
	state          RaftState
	electionTimer  time.Ticker
	heartBeatTimer time.Ticker

	electionTime time.Time

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Volatile State for leaders
	nextIndex  []int
	matchIndex []int

	// Snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// Persistent States
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)

	// Volatile States
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower

	// Volatile States for leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.GetLastLogEntry().Index + 1
		rf.matchIndex[i] = 0
	}

	rf.electionTime = time.Now().Add(GetRandomElectionTimeout())
	rf.electionTimer = *time.NewTicker(50 * time.Millisecond)
	rf.heartBeatTimer = *time.NewTicker(50 * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start separate goroutines
	go rf.heartBeatTicker()
	go rf.electionTicker()

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	go rf.applier()
	return rf
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	// 1. if rf is not leader: return false
	if !rf.IsLeader() {
		return -1, rf.GetTerm(), false
	}

	// 2. Otherwise
	// (1) Add a new logEntry to rf.log
	logEntry := rf.newLogEntry(command)
	rf.AppendLogEntry(logEntry)
	index, term := logEntry.Index, logEntry.Term
	Debug(dLog, "[S%d] adds a new logEntry of {Term: %v}, {Command %v}\n", rf.me, rf.GetTerm(), command)
	// Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
	// Debug(dLog, "[S%d] log(Command) becomes: %q", rf.me, rf.GetCommandArray())
	// (2) broadcast AppendEntry to each server
	rf.persist()
	rf.BroadcastLogReplication()

	// (3) update index, term
	return index, term, true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

// rf.me do not have any writing operation
func (rf *Raft) IsLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) GetCurState() RaftState {
	return rf.state
}

func (rf *Raft) GetVoteFor() int {
	return rf.votedFor
}

func (rf *Raft) SetVoteFor(candidate int) {
	rf.votedFor = candidate
}

func (rf *Raft) GetTerm() int {
	return rf.currentTerm
}

func (rf *Raft) GetMajority() int {
	return len(rf.peers)/2 + 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func GetTermArray(logs []LogEntry) string {
	terms := []int{}
	for _, logEntry := range logs {
		terms = append(terms, logEntry.Term)
	}
	return fmt.Sprint(terms)
}

func GetCommandArray(logs []LogEntry) string {
	cmds := []interface{}{}
	for _, logEntry := range logs {
		cmds = append(cmds, logEntry.Command)
	}
	return fmt.Sprint(cmds)
}

func (rf *Raft) GetTermArray() string {
	terms := []int{}
	for _, logEntry := range rf.logs {
		terms = append(terms, logEntry.Term)
	}
	return fmt.Sprint(terms)
}

func (rf *Raft) GetCommandArray() string {
	cmds := []interface{}{}
	for _, logEntry := range rf.logs {
		cmds = append(cmds, logEntry.Command)
	}
	return fmt.Sprint(cmds)
}

func (rf *Raft) GetNextIndex(peer int) int {
	return rf.nextIndex[peer]
}

func (rf *Raft) GetCommitIndex() int {
	return rf.commitIndex
}

func (rf *Raft) GetLastApplied() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastApplied
}

func (rf *Raft) GetMatchIndex(peer int) int {
	return rf.matchIndex[peer]
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.logs) - 1
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) SetCommitIndex(newCommitIndex int) {
	rf.commitIndex = newCommitIndex
}

func (rf *Raft) SetLastApplied(newLastApplied int) {
	rf.lastApplied = newLastApplied
}

func (rf *Raft) SetNextIndex(peer int, newNextIndex int) {
	rf.nextIndex[peer] = newNextIndex
}

func (rf *Raft) SetMatchIndex(peer int, newMatchIndex int) {
	rf.matchIndex[peer] = newMatchIndex
}
