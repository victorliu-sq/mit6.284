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
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
	// Reinitialize Volatile States for leader
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.GetLastIndex() + 1
		// Debug(dLog, "[S%d] nextIndex becomes %d", i, rf.nextIndex[i])
		rf.matchIndex[i] = rf.logStart
	}
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
}
