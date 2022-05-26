package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func GetRandomElectionTime() time.Duration {
	min := 700
	max := 800
	return time.Duration((rand.Intn(max-min+1) + min)) * time.Millisecond
}

// *************************************************************************
// Separate goroutine for Election

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) startElection() {
	// Convert to candidate and reset election timer
	rf.ConvertToCandidate()
	rf.ResetElectionTimer()
	Debug(dTerm, "[S%d] becomes {Candidate}", rf.me)
	Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	rf.BroadcastRequestVote()
}

func (rf *Raft) BroadcastRequestVote() {
	// set votes == 1
	var votes int64 = 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			Debug(dRV, "[S%d] sends RequestVote -> [S%d]\n", rf.me, peer)
			rf.RequestVoteCandidate(&votes, peer)
		}(i)
	}
}

func (rf *Raft) RequestVoteCandidate(votes *int64, peer int) {
	args := rf.newRVArgs()
	reply := rf.newRVReply()
	rf.sendRequestVote(peer, &args, &reply)

	if reply.VoteGranted {
		// If vote granted: votes++ && check if current Server becomes Leader
		atomic.AddInt64(votes, 1)
		if int(atomic.LoadInt64(votes)) == rf.GetMajority() {
			Debug(dLeader, "[S%d] becomes {Leader}\n", rf.me)
			rf.ConvertToLeader()
			rf.BroadcastHeartBeat()
		}
	}
	// Try to update Term and state after communication and reset electionTimer
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		Debug(dTerm, "[S%d] becomes {Follower}", rf.me, rf.GetTerm())
		Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		rf.ResetElectionTimer()
	}
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
