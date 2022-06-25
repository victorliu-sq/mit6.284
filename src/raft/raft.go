package raft

import (
	//	"bytes"

	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labrpc"
)

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

	rf.logStart = 0

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
	Debug(dSnap, "[S%v] restores", rf.me)
	rf.readPersist(persister.ReadRaftState())

	// start separate goroutines
	go rf.heartBeatTicker()
	go rf.electionTicker()

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	go rf.applier()
	return rf
}

// ---------------------------------------------------------------------------------------------------------

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent State
	currentTerm int
	votedFor    int
	logs        []LogEntry

	logStart int

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
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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
