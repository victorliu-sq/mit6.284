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

	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	log         []LogEntry

	// Volatile State
	commitIndex int
	lastApplied int

	// DIY
	state          RaftState
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	// heartBeatCond  *sync.Cond
	replicatorCond []*sync.Cond

	electionTime time.Time

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Volatile State for leaders
	nextIndex  []int
	matchIndex []int
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
	rf.log = make([]LogEntry, 1)

	// Volatile States
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.electionTimer = time.NewTimer(GetRandomElectionTime())
	rf.heartBeatTimer = time.NewTimer(GetHeartBeatTime())
	rf.replicatorCond = make([]*sync.Cond, len(peers))

	// Volatile States for leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.GetLastLogEntry().Index + 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start separate goroutines
	go rf.ticker()

	rf.applyCond = sync.NewCond(&sync.Mutex{})
	rf.applyCh = applyCh
	// go rf.applier()

	// rf.electionTime = time.Now()

	// HeartBeatSender
	// rf.heartBeatCond = sync.NewCond(&sync.Mutex{})
	// go rf.HeartBeastSender()
	// LogReplicationSender
	// for peer, _ := range peers {
	// 	rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
	// 	go rf.LogReplicationSender(peer)
	// }

	return rf
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

func (rf *Raft) ResetElectionTimer() {
	rf.electionTimer.Reset(GetRandomElectionTime())
}

func (rf *Raft) ResetHeartBeatTimer() {
	rf.heartBeatTimer.Reset(GetHeartBeatTime())
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

func (rf *Raft) GetTermArray() string {
	terms := []int{}
	for _, logEntry := range rf.log {
		terms = append(terms, logEntry.Term)
	}
	return fmt.Sprint(terms)
}

func (rf *Raft) GetCommandArray() string {
	cmds := []interface{}{}
	for _, logEntry := range rf.log {
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

func (rf *Raft) AdvanceCommitIndexFollower(LeaderCommit int) {
	if LeaderCommit > rf.GetCommitIndex() {
		newCommitIndex := min(LeaderCommit, rf.GetLastLogEntry().Index)
		rf.SetCommitIndex(newCommitIndex)
		Debug(dLog, "[S%d](Follower)'s Commmit Index becomes %d", rf.me, newCommitIndex)
	}
}

func (rf *Raft) AdvanceCommitIndexLeader() {
	if !rf.IsLeader() {
		return
	}

	for N := rf.GetCommitIndex() + 1; N <= rf.GetLastLogEntry().Index; N++ {
		if rf.GetLogEntry(N).Term == rf.GetTerm() {
			num := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.GetMatchIndex(peer) >= N {
					Debug(dLog, "[S%d] tries to increment Commit Index to %d, Checking [S%d]", rf.me, N, peer)
					num++
					if num == rf.GetMajority() {
						rf.SetCommitIndex(N)
						Debug(dLog, "[S%d](Leader)'s Commmit Index becomes %d", rf.me, N)
						break
					}
				}
			}
		}
	}
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
}

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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 1. if rf is not leader: return false
	if !rf.IsLeader() {
		isLeader = false
		return index, term, isLeader
	}

	// 2. Otherwise
	// (1) Add a new logEntry to rf.log
	logEntry := rf.newLogEntry(command)
	rf.AppendLogEntry(logEntry)
	index, term = logEntry.Index, logEntry.Term
	Debug(dLog, "[S%d] adds a new logEntry of {Term: %v}, {Command %v}\n", rf.me, rf.GetTerm(), command)
	Debug(dLog, "[S%d] log becomes: %q", rf.me, rf.GetTermArray())
	// Debug(dLog, "index is %d", index)
	// (2) broadcast AppendEntry to each server
	rf.BroadcastLogReplication()

	// (3) update index, term
	return index, term, isLeader
}

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

// func getRandomHeartBeatTime() time.Duration {
// 	return time.Duration((rand.Intn(100-50+1) + 50)) * time.Millisecond
// }

// func (rf *Raft) applier() {
// 	for rf.killed() == false {
// 		rf.applyCond.L.Lock()
// 		defer rf.applyCond.L.Unlock()
// 		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
// 		for rf.GetLastApplied() >= rf.GetCommitIndex() {
// 			rf.applyCond.Wait()
// 		}
// 		entries := make([]LogEntry, rf.GetCommitIndex()-rf.GetLastApplied())
// 		copy(entries, rf.log[rf.GetLastApplied()+1:rf.GetCommitIndex()+1])
// 		for _, logEntry := range entries {
// 			rf.applyCh <- ApplyMsg{
// 				CommandValid: true,
// 				Command:      logEntry.Command,
// 				CommandIndex: logEntry.Index,
// 			}
// 		}
// 		Debug(dLog, "[S%d] applies entries %d ~ %d in term %d", rf.me, rf.GetLastApplied(), rf.GetCommitIndex(), rf.GetTerm())
// 		// DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
// 		rf.SetLastApplied(max(rf.GetLastApplied(), rf.GetCommitIndex()))
// 	}
// }
