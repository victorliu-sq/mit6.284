package raft

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate RaftState = "Candidate"
	Leader    RaftState = "Leader"
)

// if a's term < b's term, a should convert back to follower
// Notice that a's term does not mean a's last log's term
func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) ConvertToLeader() {
	rf.mu.RLock()
	LastLogIndex := rf.GetLastLogEntry().Index
	rf.mu.RUnlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	// Reinitialize Volatile States for leader
	for i, _ := range rf.peers {
		rf.nextIndex[i] = LastLogIndex + 1
		// Debug(dLog, "[S%d] nextIndex becomes %d", i, rf.nextIndex[i])
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) ConvertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
}
