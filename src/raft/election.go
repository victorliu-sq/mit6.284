package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// *************************************************************************
// Separate goroutine for Election

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartBeatTimer.C:
			_, isleader := rf.GetState()
			if isleader {
				rf.broadcastHeartBeat()
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func getBroadCastTime() time.Duration {
	return 150 * time.Microsecond
}

func getRandomElectionTime() time.Duration {
	min := 300
	max := 500
	return time.Duration((rand.Intn(max-min+1) + min)) * time.Millisecond
}

func (rf *Raft) startElection() {
	// Convert to candidate and reset election timer
	rf.ConvertToCandidate()
	rf.ResetElectionTimer()
	Debug(dTerm, "[S%d] becomes {Candidate}", rf.me)
	Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())

	// set votes == 1
	var votes int64 = 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			Debug(dRV, "[S%d] sends RequestVote -> [S%d]\n", rf.me, peer)

			args := rf.newRVArgs()
			reply := rf.newRVReply()
			rf.sendRequestVote(peer, &args, &reply)

			if reply.VoteGranted {
				// If vote granted: votes++ && check if current Server becomes Leader
				atomic.AddInt64(&votes, 1)
				if int(atomic.LoadInt64(&votes)) == rf.GetMajority() {
					Debug(dLeader, "[S%d] becomes {Leader}\n", rf.me)
					rf.ConvertToLeader()
					rf.broadcastHeartBeat()
				}
			}
			// Try to update Term and state after communication and reset electionTimer
			if reply.Term > rf.GetTerm() {
				rf.ConvertToFollower(reply.Term)
				Debug(dTerm, "[S%d] becomes {Follower}", rf.me, rf.GetTerm())
				Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
				rf.ResetElectionTimer()
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartBeat() {
	// send heartBeats to each server
	Debug(dTimer, "[S%d] broadcasts HeartBeats\n", rf.me)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.AppendEntryLeader(peer, true)
			// args := rf.newAEArgs(true)
			// reply := rf.newAEReply()
			// rf.sendAppendEntry(server_idx, &args, &reply)
		}(i)
	}
}
