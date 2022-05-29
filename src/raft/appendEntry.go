package raft

import "time"

const heartBeatTickTime = 150 * time.Millisecond

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
	// Debug(dTimer, "[S%d] broadcasts Log Replication\n", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.IsLeader() && (rf.GetNextIndex(peer) <= rf.GetLastLogEntry().Index) {
			// Debug(dHB, "[S%v]'s log length is %v", rf.me, len(rf.logs))
			args := rf.newAEArgs(peer)
			reply := rf.newAEReply()
			go rf.AppendEntryLeader(peer, &args, &reply)
		}
	}
}

func (rf *Raft) AppendEntryLeader(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.sendAppendEntry(peer, args, reply) {
		return
	}
	if len(args.Entries) > 0 {
		Debug(dLog, "[S%v] sends AE to [S%v] with prevLogIndex: %d, prevLogTerm: %d", rf.me, peer, args.PrevLogIndex, args.PrevLogTerm)
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

	// 4. If there exists an N such that N > commitIndex, a majority of matchIOndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	rf.AdvanceCommitIndexLeader()
	// Debug(dLog, "[S%d](Leader)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
	rf.applyCond.Signal()
}

func (rf *Raft) ProcessReply(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if reply.Success {
		// contain and match
		// Debug(dHB, "[S%v] gets AE reply from [S%v]: {contain AND match}", rf.me, peer)
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.GetNextIndex(peer) {
			// Debug(dHB, "[S%v]'s nextIndex becomes %v", rf.me, args.PrevLogIndex+len(args.Entries)+1)
			rf.SetNextIndex(peer, newNext)
			// Debug(dTimer, "[S%d] changes nextIndex of [S%d] to %d\n", rf.me, peer, newMatch)
		}

		if newMatch > rf.GetMatchIndex(peer) {
			rf.SetMatchIndex(peer, newMatch)
			// Debug(dTimer, "[S%d] changes matchIndex of [S%d] to %d\n", rf.me, peer, newMatch)
		}
	} else if reply.XValid {
		// (contain but mismatch)
		if len(args.Entries) > 0 {
			// Debug(dHB, "[S%v] gets AE reply from [S%v]: {contain BUT mismatch, XIndex: %v, XTerm: %v}", rf.me, peer, reply.XIndex, reply.XTerm)
		}
		oldNext := args.PrevLogIndex + 1
		// newNext := rf.GetXIndexLeader(reply.XTerm, reply.XIndex)
		newNext := reply.XIndex
		// Debug(dHB, "[S%v]{Leader-mismatch}: oldNextIndex:%v, newNextIndex:%v, prevLogTerm:%v", rf.me, oldNext, newNext, args.PrevLogTerm)
		// rf.SetNextIndex(peer, newNext)
		// newNext := reply.XIndex
		if newNext < oldNext {
			rf.SetNextIndex(peer, newNext)
		}
	} else {
		// not contain
		// Debug(dHB, "[S%v] gets AE reply from [S%v]: {not contain} ", rf.me, peer)
		// rf.SetNextIndex(peer, reply.XIndex)
		newNext := reply.XIndex
		// rf.SetNextIndex(peer, newNext)
		if newNext < rf.GetNextIndex(peer) {
			rf.SetNextIndex(peer, newNext)
		}
		// oldNext := args.PrevLogIndex + 1
		// if oldNext > 1 {
		// 	rf.SetNextIndex(peer, min(oldNext-1, reply.XIndex))
		// }
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
			Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		}
		if args.Term > term {
			Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		}
	}

	// 2.Rely false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// Discuss whether conflict
	Debug(dLog, "[S%v] receives prevLogIndex: %d, prevLogTerm: %d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	Debug(dLog, "[S%v] receives log Entry of length %v", rf.me, len(args.Entries))
	if len(args.Entries) > 0 {
		// Debug(dLog, "[S%d] receives log Entry(Term): %v", rf.me, GetTermArray(args.Entries))
		// Debug(dLog, "[S%d] receives log Entry(Command): %v", rf.me, GetCommandArray(args.Entries))
	}
	if !rf.ContainAndMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex > rf.GetLastLogEntry().Index {
			// not contain --> return length of logs
			reply.XValid = false
			reply.XIndex = len(rf.logs)
			Debug(dHB, "[S%v] length is %v, does not contain an element at %v", rf.me, len(rf.logs), args.PrevLogIndex)
		} else {
			// contain but mismatch --> conflict
			// 3. if an existing entry conflicts with a new one, delete the existing entry and all that follow it
			reply.XValid = true
			reply.XTerm = rf.GetLogEntry(args.PrevLogIndex).Term
			reply.XIndex = rf.GetXIndex(args.PrevLogIndex, reply.XTerm)
			// Debug(dHB, "[S%v] replies XTerm:%v, XIndex:%v", rf.me, reply.XTerm, reply.XIndex)
		}
	} else {
		// 4. Append any new entries not already in the log
		reply.Success = true
		reply.XValid = false
		rf.AppendNewEntries(args.PrevLogIndex, args.Entries)
		rf.persist()

		// 5. Advance Commit Index for Follower
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

		rf.AdvanceCommitIndexFollower(args.LeaderCommit)

		rf.applyCond.Signal()
		if len(args.Entries) > 0 {
			// Debug(dLog, "[S%d] log(Term) becomes: %v", rf.me, rf.GetTermArray())
			// Debug(dLog, "S%d log(Command) becomes: %v", rf.me, rf.GetCommandArray())
			// Debug(dLog, "[S%d](Follower)'s Commit Index is %d\n", rf.me, rf.GetCommitIndex())
		}
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
	// Debug(dHB, "Leader CommitIndex:%v, [S%v]'s CommitIndex:%v ", LeaderCommit, rf.me, rf.GetCommitIndex())
	if LeaderCommit > rf.GetCommitIndex() {
		newCommitIndex := min(LeaderCommit, rf.GetLastLogEntry().Index)
		rf.SetCommitIndex(newCommitIndex)
		// Debug(dLog, "[S%d](Follower)'s Commmit Index becomes %d", rf.me, newCommitIndex)
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
						// Debug(dLog, "[S%d](Leader)'s Commmit Index becomes %d", rf.me, N)
						break
					}
				}
			}
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = 0
	// we do not apply first log entry, namely {Command:<nil>, Term:0}

	for !rf.killed() {
		if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.GetLastLogEntry().Index {
			rf.lastApplied++
			// term, cmd, cmdIdx := rf.GetLogEntry(rf.lastApplied).Term, rf.GetLogEntry(rf.lastApplied).Command, rf.GetLogEntry(rf.lastApplied).Index
			// Debug(dLog, "[S%d] applies Log Entry [{Term: %v}{Command: %v}]", rf.me, term, cmd)

			cmd, cmdIdx := rf.GetLogEntry(rf.lastApplied).Command, rf.GetLogEntry(rf.lastApplied).Index
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: cmdIdx,
			}
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// *************************************************************************************
type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Success bool
	Term    int

	// Conflict information
	XValid bool // whether conflict
	XTerm  int  // the term conflicting the prevLogTerm
	XIndex int  // index of first entry of Xterm
}

func (rf *Raft) newAEArgs(peer int) AppendEntryArgs {
	// If prev leader's log is very long but most of log entries are not replicated
	// Since nextIndex is optimistic, it can easily go out of range
	next := rf.nextIndex[peer]

	if next <= 0 {
		next = 1
	} else if next > rf.GetLastIndex()+1 {
		next = rf.GetLastIndex() + 1
	}

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.logs[next-1].Term,
		Entries:      make([]LogEntry, rf.GetLastLogEntry().Index-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.GetSubarrayEnd(next))
	// if len(args.Entries) == 0 {
	// 	Debug(dLog, "HeartBeart Msg\n")
	// } else {
	// 	Debug(dLog, "Replication Msg\n")
	// }
	return args
}

func (rf *Raft) newAEReply() AppendEntryReply {
	return AppendEntryReply{}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
