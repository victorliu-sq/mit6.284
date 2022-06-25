package raft

import (
	"math/rand"
	"time"
)

// *************************************************************************
// Election Ticker
const electionTickTime = 30 * time.Microsecond

func (rf *Raft) electionTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
	} else {
		if time.Now().After(rf.electionTime) {
			rf.startElection()
		}
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		rf.electionTick()
		time.Sleep(electionTickTime)
	}
}

func GetRandomElectionTimeout() time.Duration {
	min := 1000
	max := 1300
	return time.Duration((rand.Intn(max-min+1) + min)) * time.Millisecond
}

func (rf *Raft) SetElectionTime() {
	t := time.Now()
	t = t.Add(GetRandomElectionTimeout())
	rf.electionTime = t
}

func (rf *Raft) startElection() {
	// 1. Conversion to Candidate
	// (1) increment current Term (2) Vote for itself (3)Reset election timer (4) State = Candidate
	rf.ConvertToCandidate()
	rf.SetElectionTime()

	// Debug(dTerm, "[S%d] becomes {Candidate}", rf.me)
	// Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	// (4) Send RequestVote RPCs to all other servers
	rf.BroadcastRequestVote()
}

func (rf *Raft) BroadcastRequestVote() {
	// set votes == 1
	votes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// Debug(dRV, "[S%d] sends RequestVote -> [S%d]\n", rf.me, peer)
		args := rf.newRVArgs()
		reply := rf.newRVReply()
		go rf.RequestVoteSender(&votes, peer, &args, &reply)
	}
}

// *************************************************************************
// RequestVote Sender

func (rf *Raft) RequestVoteSender(votes *int, peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if !rf.sendRequestVote(peer, args, reply) {
		// Debug(dRV, "[S%v] gets No feedback of RV from [S%v]", rf.me, peer)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Rule for all servers
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		// rf.SetElectionTime()
	}

	if reply.VoteGranted {
		// If votes received from majority of servers: become Leader
		(*votes)++
		// Debug(dLeader, "[S%d] get (%d) votes now\n", rf.me, *votes)
		if *votes == rf.GetMajority() {
			Debug(dLeader, "[S%v] becomes {Leader}\n", rf.me)
			rf.ConvertToLeader()
			// Upon election: send heartbeat to each server
			rf.BroadcastAppendEntry(true)
		}
	}
}

// **************************************************************************
// RequestVote Receiver
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers
	// Try to update Term and state after communication BUT only reset timer if vote granted
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
		// Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		// Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
	}

	if args.Term < rf.GetTerm() {
		// 1. Reply false if term < currentTerm
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
		// Debug(dRV, "[S%d] refused Vote -> [S%d]", rf.me, args.CandidateId)
	} else if (rf.GetVoteFor() == -1 || rf.GetVoteFor() == args.CandidateId) &&
		rf.checkUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		// Debug(dRV, "[S%d] Got Vote <- [S%d]\n", args.CandidateId, rf.me)
		reply.Term = rf.GetTerm()
		reply.VoteGranted = true
		rf.SetVoteFor(args.CandidateId)
		rf.SetElectionTime()
		rf.persist()
	} else {
		// Otherwise, reply false as 1.
		// Debug(dRV, "[S%d] refused Vote -> [S%d]", rf.me, args.CandidateId)
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
	}
}

// *************************************************************************************
// RequestVote Struct
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) newRVArgs() RequestVoteArgs {
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastLogEntry().Index,
		LastLogTerm:  rf.GetLastLogEntry().Term,
	}
}

func (rf *Raft) newRVReply() RequestVoteReply {
	return RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
