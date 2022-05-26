package raft

import "time"

func GetHeartBeatTime() time.Duration {
	return 50 * time.Microsecond
}

// Separate goroutine for HeartBeat
func (rf *Raft) BroadcastHeartBeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// Debug(dTimer, "[S%d] sends HeartBeat -> [S%d]\n", rf.me, peer)
		args := rf.newAEArgs(peer)
		reply := rf.newAEReply()
		go rf.AppendEntryLeader(peer, &args, &reply)
	}
}

// Separate goroutine for LogReplication
// func (rf *Raft) BroadcastLogReplication() {
// 	// log replication for each server
// 	Debug(dTimer, "[S%d] broadcasts Log Replication\n", rf.me)
// 	for peer := range rf.peers {
// 		if peer == rf.me {
// 			continue
// 		}
// 		go func(i int) {
// 			rf.replicatorCond[i].Signal()
// 			// rf.AppendEntryLeader(i)
// 		}(peer)
// 	}
// }

// func (rf *Raft) LogReplicationSender(peer int) {
// 	for !rf.killed() {
// 		rf.replicatorCond[peer].L.Lock()
// 		defer rf.replicatorCond[peer].L.Unlock()
// 		for !(rf.IsLeader() && (rf.GetNextIndex(peer) <= rf.GetLastLogEntry().Index)) {
// 			rf.replicatorCond[peer].Wait()
// 		}

// 		Debug(dTimer, "[S%d] sends Log Replication -> [S%d]\n", rf.me, peer)
// 		// rf.AppendEntryLeader(peer)
// 	}
// }

// **************************************************************************
// AppendEntry Sender
func (rf *Raft) AppendEntryLeader(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.sendAppendEntry(peer, args, reply) {
		return
	}
	// 3. Process Reply
	// rf.ProcessReply(peer, args, reply)

	// 4. If there exists an N such taht N > commitIndex, a majority of matchIOndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	// rf.AdvanceCommitIndexLeader()
	// Debug(dLog, "[S%d](Leader)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
	// rf.applyCond.Signal()
}

/* func (rf *Raft) ProcessReply(peer int, args AppendEntryArgs, reply AppendEntryReply) {
	if reply.Success {
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)

		if newNext > rf.GetNextIndex(peer) {
			rf.SetNextIndex(peer, newNext)
			Debug(dTimer, "[S%d] changes nextIndex of [S%d] to %d\n", rf.me, peer, newMatch)
		}

		if newMatch > rf.GetMatchIndex(peer) {
			rf.SetMatchIndex(peer, newMatch)
			Debug(dTimer, "[S%d] changes matchIndex of [S%d] to %d\n", rf.me, peer, newMatch)
		}
		// rf.SetNextIndex(peer, newNext)
		// rf.SetMatchIndex(peer, newMatch)
	} else if reply.XValid {
		newNext := reply.XIndex
		rf.SetNextIndex(peer, newNext)
		rf.AppendEntryLeader(peer)
	}
} */

// AppendEntry Receiver
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	// No conflict
	/* if args.Term < rf.GetTerm() {
		reply.XValid = false
		reply.Success = false
		return
	} */

	// HeartBeat Msg
	term := rf.GetTerm()
	isLeader := rf.GetCurState() == Leader
	rf.ConvertToFollower(max(args.Term, term))
	// Debug(dLog, "[S%d] Election Timer Reset", rf.me)
	// Debug(dTimer, "[S%d]'s Election Timer is Reset", rf.me)
	// rf.ResetElectionTimer()
	rf.SetElectionTime()

	if args.Term > term || isLeader {
		if isLeader {
			Debug(dTerm, "[S%d] becomes {Follower}", rf.me, rf.GetTerm())
		}
		if args.Term > term {
			Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		}
	}

	// 2.Rely false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// Discuss whether conflict
	// Debug(dLog, "prevLogIndex: %d, prevLogTerm: %d", args.PrevLogIndex, args.PrevLogTerm)
	// if len(args.Entries) > 0 {
	// 	Debug(dLog, "[S%d] receives Entry with {Command %d}, {Term: %v}", rf.me, args.Entries[0].Command, args.Entries[0].Term)
	// }
	// if !rf.ContainAndMatch(args.PrevLogIndex, args.PrevLogTerm) {
	// 	reply.Success = false
	// 	if args.PrevLogIndex > rf.GetLastLogEntry().Index {
	// 		// not contain
	// 		reply.XValid = false
	// 	} else {
	// 		// contain but mismatch --> conflict
	// 		// 3. if an existing entry conflicts with a new one, delete the existing entry and all that follow it
	// 		reply.XValid = true
	// 		reply.XTerm = args.PrevLogTerm
	// 		reply.XIndex = rf.GetXIndex(args.PrevLogIndex, args.PrevLogTerm)
	// 	}
	// 	return
	// }

	// 4. Append any new entries not already in the log
	// reply.Success = true
	// reply.XValid = false
	// rf.AppendNewEntries(args.PrevLogIndex, args.Entries)
	// Debug(dLog, "S%d log becomes: %q", rf.me, rf.GetTermArray())

	// Debug(dLog, "S%d log(Term) becomes: %q", rf.me, rf.GetTermArray())

	// Debug(dLog, "S%d log(Command) becomes: %q", rf.me, rf.GetCommandArray())

	// 5. Advance Commit Index for Follower
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// rf.AdvanceCommitIndexFollower(args.LeaderCommit)

	// Debug(dLog, "[S%d](Follower)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
}

// func (rf *Raft) ContainAndMatch(prevLogIndex int, prevLogTerm int) bool {
// 	if prevLogIndex <= rf.GetLastLogEntry().Index && prevLogTerm == rf.GetLogEntry(prevLogIndex).Term {
// 		return true
// 	} else {
// 		return false
// 	}
// }
