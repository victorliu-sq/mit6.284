package raft

import "time"

func (rf *Raft) heartBeatTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
		rf.BroadcastHeartBeat()
	}
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		rf.heartBeatTick()
		time.Sleep(heartBeatTickTime)
	}
}

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
func (rf *Raft) BroadcastLogReplication() {
	// log replication for each server
	Debug(dTimer, "[S%d] broadcasts Log Replication\n", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.IsLeader() && (rf.GetNextIndex(peer) < rf.GetLastLogEntry().Index) {
			go func(i int) {
				args := rf.newAEArgs(i)
				reply := rf.newAEReply()
				rf.AppendEntryLeader(i, &args, &reply)
			}(peer)
		}
	}
}

func (rf *Raft) AppendEntryLeader(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.sendAppendEntry(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.IsLeader() || rf.GetTerm() != args.Term {
		return
	}

	// Rule for all server
	// Process Reply -- all server
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		// rf.SetElectionTime()
		Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		return
	}

	// 3. Process Reply
	rf.ProcessReply(peer, args, reply)

	// 4. If there exists an N such taht N > commitIndex, a majority of matchIOndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	rf.AdvanceCommitIndexLeader()
	// Debug(dLog, "[S%d](Leader)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
	rf.applyCond.Signal()
}

func (rf *Raft) ProcessReply(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if reply.Success {
		// contain and match
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
	} else if reply.XValid {
		// (contain but mismatch)
		newNext := reply.XIndex
		rf.SetNextIndex(peer, newNext)
		// }
	} else {
		// not contain
		rf.SetNextIndex(peer, min(len(rf.logs), reply.XIndex))
	}
}

// **************************************************************************
// AppendEntry Receiver
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// HeartBeat Msg
	term := rf.GetTerm()
	isLeader := rf.GetCurState() == Leader
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
	}
	// Debug(dTimer, "[S%d]'s Election Timer is Reset", rf.me)
	rf.SetElectionTime()

	// 1. Reply false if term < currentTerm
	// No conflict
	// if rf.me == 0 {
	// Debug(dHB, "[S%v]'s curTerm is %v and it receives Command of term %v", rf.me, rf.currentTerm, args.Term)
	// }
	reply.Term = rf.GetTerm()
	if args.Term < rf.GetTerm() {
		reply.XValid = false
		reply.Success = false
		return
	}

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
	if len(args.Entries) > 0 {
		Debug(dLog, "[S%d] receives log Entry(Term): %v", rf.me, GetTermArray(args.Entries))
		Debug(dLog, "[S%d] receives log Entry(Command): %v", rf.me, GetCommandArray(args.Entries))
	}
	if !rf.ContainAndMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex > rf.GetLastLogEntry().Index {
			// not contain --> return last index + 1
			reply.XValid = false
			reply.XIndex = rf.GetLastLogEntry().Index + 1
			reply.Term = rf.GetLastLogEntry().Term
			Debug(dHB, "[S%v] does not contain an element at %v", rf.me, args.PrevLogIndex)
		} else {
			// contain but mismatch --> conflict
			// 3. if an existing entry conflicts with a new one, delete the existing entry and all that follow it
			reply.XValid = true
			reply.XTerm = args.PrevLogTerm
			reply.XIndex = rf.GetXIndex(args.PrevLogIndex, args.PrevLogTerm)
			Debug(dHB, "[S%v] replies XTerm:%v, XIndex:%v", reply.XTerm, reply.XIndex)
		}
		return
	}

	// 4. Append any new entries not already in the log
	reply.Success = true
	reply.XValid = false
	rf.AppendNewEntries(args.PrevLogIndex, args.Entries)

	// 5. Advance Commit Index for Follower
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.AdvanceCommitIndexFollower(args.LeaderCommit)

	rf.applyCond.Signal()
	if len(args.Entries) > 0 {
		Debug(dLog, "S%d log(Term) becomes: %v", rf.me, rf.GetTermArray())
		Debug(dLog, "S%d log(Command) becomes: %v", rf.me, rf.GetCommandArray())
		Debug(dLog, "[S%d](Follower)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
	}
}

func (rf *Raft) ContainAndMatch(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex <= rf.GetLastLogEntry().Index && prevLogTerm == rf.GetLogEntry(prevLogIndex).Term {
		return true
	} else {
		return false
	}
}

func (rf *Raft) AdvanceCommitIndexFollower(LeaderCommit int) {
	Debug(dHB, "Leader CommitIndex:%v, [S%v]'s CommitIndex:%v ", LeaderCommit, rf.me, rf.GetCommitIndex())
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
					// Debug(dLog, "[S%d] tries to increment Commit Index to %d, Checking [S%d]", rf.me, N, peer)
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
