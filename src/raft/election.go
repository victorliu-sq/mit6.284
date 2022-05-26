package raft

import (
	"math/rand"
	"time"
)

const electionTickTime = 100 * time.Microsecond

const heartBeatTickTime = 50 * time.Millisecond

func GetNextElectionTime() time.Duration {
	min := 1000
	max := 1300
	return time.Duration((rand.Intn(max-min+1) + min)) * time.Millisecond
}

// *************************************************************************
// Separate goroutine for Election

func (rf *Raft) SetElectionTime() {
	t := time.Now()
	t = t.Add(GetNextElectionTime())
	rf.electionTime = t
}

func (rf *Raft) electionTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Now().After(rf.electionTime) {
		rf.startElection()
	}
}

func (rf *Raft) heartBeatTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
		rf.BroadcastHeartBeat()
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		rf.electionTick()
		time.Sleep(electionTickTime)
	}
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		rf.heartBeatTick()
		time.Sleep(heartBeatTickTime)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) startElection() {
	// Convert to candidate and reset election timer
	rf.ConvertToCandidate()
	rf.SetElectionTime()

	Debug(dTerm, "[S%d] becomes {Candidate}", rf.me)
	Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	rf.BroadcastRequestVote()
}

func (rf *Raft) BroadcastRequestVote() {
	// set votes == 1
	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			Debug(dRV, "[S%d] sends RequestVote -> [S%d]\n", rf.me, peer)
			args := rf.newRVArgs()
			reply := rf.newRVReply()
			rf.RequestVoteCandidate(&votes, peer, &args, &reply)
		}(i)
	}
}

func (rf *Raft) RequestVoteCandidate(votes *int, peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if !rf.sendRequestVote(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Rule for all servers
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		rf.SetElectionTime()

		Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	}

	if reply.VoteGranted {
		// If vote granted: votes++ && check if current Server becomes Leader
		(*votes)++
		Debug(dLeader, "[S%d] get (%d) votes now\n", rf.me, *votes)
		if *votes == rf.GetMajority() {
			Debug(dLeader, "[S%d] becomes {Leader}\n", rf.me)
			rf.ConvertToLeader()
			rf.BroadcastHeartBeat()
		}
	}
}

// **************************************************************************
// RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// receiver's implementation
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers
	// Try to update Term and state after communication BUT only reset timer if vote granted
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
		Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	}

	if args.Term < rf.GetTerm() {
		// 1. Reply false if term < currentTerm
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
		Debug(dRV, "[S%d] refused Vote -> [S%d]", rf.me, args.CandidateId)
	} else if (rf.GetVoteFor() == -1 || rf.GetVoteFor() == args.CandidateId) &&
		rf.checkUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		Debug(dRV, "[S%d] Got Vote <- [S%d]\n", args.CandidateId, rf.me)
		reply.Term = rf.GetTerm()
		reply.VoteGranted = true

		Debug(dTimer, "[S%d]'s Election Timer is Reset", rf.me)
		rf.SetVoteFor(args.CandidateId)
		rf.SetElectionTime()
	} else {
		// Debug(dRV, "Otherwise")
		Debug(dRV, "[S%d] refused Vote -> [S%d]", rf.me, args.CandidateId)
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
	}
}
