package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) newRVArgs() RequestVoteArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastLogEntry().Index,
		LastLogTerm:  rf.GetLastLogEntry().Term,
	}
}

func (rf *Raft) newRVReply() RequestVoteReply {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// **************************************************************************
// RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// receiver's implementation

	// 1. Reply false if term < currentTerm
	if args.Term < rf.GetTerm() {
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
		Debug(dRV, "[S%d] refused Vote -> [S%d]", rf.me, args.CandidateId)
		return
	}

	// Try to update Term and state after communication
	// BUT only reset timer if vote granted
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
		Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.GetVoteFor() == -1 || rf.GetVoteFor() == args.CandidateId) &&
		checkUpToDate(args.LastLogIndex, args.LastLogTerm, rf.GetLastLogEntry().Index, rf.GetLastLogEntry().Term) {
		rf.SetCandidate(args.CandidateId)
		reply.Term = rf.GetTerm()
		reply.VoteGranted = true
		Debug(dRV, "[S%d] Got Vote <- [S%d]\n", args.CandidateId, rf.me)
		// Reset Timer if VoteGranted
		rf.ResetElectionTimer()
	}
}
