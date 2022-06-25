package raft

import "time"

// *************************************************************************
// HeartBeat Ticker
const heartBeatTickTime = 150 * time.Millisecond

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		rf.heartBeatTick()
		time.Sleep(heartBeatTickTime)
	}
}

func (rf *Raft) heartBeatTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
		rf.BroadcastAppendEntry(true)
	}
}

// *************************************************************************
// Append a new Command
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. if rf is not leader: return false
	if !rf.IsLeader() {
		return -1, rf.GetTerm(), false
	}

	// 2. Otherwise
	// (1) Add a new logEntry to rf.log
	logEntry := rf.newLogEntry(command)
	rf.AppendLogEntry(logEntry)
	index, term := logEntry.Index, logEntry.Term
	// (2) broadcast AppendEntry to each server
	rf.persist()
	rf.BroadcastAppendEntry(false)

	// (3) update index, term
	return index, term, true
}

// *************************************************************************
// Broadcast AppendEntry
func (rf *Raft) BroadcastAppendEntry(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if isHeartBeat || rf.GetNextIndex(peer) <= rf.GetLastIndex() {
			args := rf.newAEArgs(peer)
			reply := rf.newAEReply()

			go rf.AppendEntrySender(peer, &args, &reply)
		}
	}
}

// *************************************************************************
// AppendEntry Sender
func (rf *Raft) AppendEntrySender(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	// AE
	if !rf.sendAppendEntry(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all server
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		// Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		return
	}

	// 3. Process Reply
	rf.ProcessReply(peer, args, reply)

	// 4. Advance commit index of leader
	rf.AdvanceCommitIndexLeader()
}

func (rf *Raft) ProcessReply(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.IsLeader() || rf.GetTerm() != args.Term {
		return
	}

	if reply.Success {
		// contain and match
		// Debug(dHB, "[S%v] gets AE reply from [S%v]: {contain AND match}", rf.me, peer)
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.GetNextIndex(peer) {
			rf.SetNextIndex(peer, newNext)
		}

		if newMatch > rf.GetMatchIndex(peer) {
			rf.SetMatchIndex(peer, newMatch)
		}
	} else if reply.XValid {
		// (contain but mismatch)
		oldNext := args.PrevLogIndex + 1
		newNext := reply.XIndex
		// Debug(dHB, "[S%v]{Leader-mismatch}: oldNextIndex:%v, newNextIndex:%v, prevLogTerm:%v", rf.me, oldNext, newNext, args.PrevLogTerm)
		if newNext < oldNext {
			rf.SetNextIndex(peer, newNext)
		}

		// resend AE
		// args := rf.newAEArgs(peer)
		// reply := rf.newAEReply()

		// go rf.AppendEntrySender(peer, &args, &reply)
	} else {
		// not contain
		// Debug(dHB, "[S%v] gets AE reply from [S%v]: {not contain} ", rf.me, peer)
		newNext := reply.XIndex
		if newNext < rf.GetNextIndex(peer) {
			rf.SetNextIndex(peer, newNext)
		}

		if rf.GetNextIndex(peer) <= rf.GetFirstIndex() {
			// Debug(dSnap, "[S%v]'s next Index is %v, start index is %v", peer, rf.GetNextIndex(peer), rf.GetFirstIndex())
			snap_args := rf.NewInstallSnapshotArgs()
			snap_reply := rf.NewInstallSnapshotReply()
			go rf.InstallSnapshotSender(peer, &snap_args, &snap_reply)
		}
	}
}

func (rf *Raft) AdvanceCommitIndexLeader() {
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	if !rf.IsLeader() {
		return
	}
	Debug(dSnap, "[S%v]'s commit Index is %v", rf.me, rf.GetCommitIndex())
	for N := rf.GetCommitIndex() + 1; N <= rf.GetLastIndex(); N++ {
		if rf.GetLogEntry(N).Term == rf.GetTerm() {
			num := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.GetMatchIndex(peer) >= N {
					num++
					if num == rf.GetMajority() {
						rf.SetCommitIndex(N)
						// apply
						rf.applyCond.Signal()
						break
					}
				}
			}
		}
	}
}

// **************************************************************************
// AppendEntry Receiver
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.GetTerm()
	isLeader := rf.GetCurState() == Leader

	// Rule for all server
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
	}

	// HeartBeat Msg
	rf.SetElectionTime()

	// 1. Reply false if term < currentTerm
	// No conflict
	reply.Term = rf.GetTerm()
	if args.Term < rf.GetTerm() {
		reply.XValid = false
		reply.Success = false
		return
	}

	if args.Term > term || isLeader {
		if isLeader {
			// Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		}
		if args.Term > term {
			// Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		}
	}

	// 2.Rely false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.ContainAndMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex > rf.GetLastIndex() || args.PrevLogIndex < rf.GetFirstIndex() {
			// not contain --> return length of logs
			// lag logEntries due to crash or snapshot
			reply.XIndex = rf.GetLastIndex() + 1
			reply.XValid = false
		} else {
			// contain but mismatch --> conflict
			// 3. if an existing entry conflicts with a new one, delete the existing entry and all that follow it
			// Debug(dSnap, "[S%v]'s first index:%v, last index:%v", rf.me, rf.GetFirstIndex(), rf.GetLastIndex())
			// Debug(dSnap, "[S%v]'s PrevLogIndex is %v", rf.me, args.PrevLogIndex)
			// Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
			reply.XTerm = rf.GetLogEntry(args.PrevLogIndex).Term
			reply.XIndex = rf.GetXIndex(args.PrevLogIndex, reply.XTerm)
			reply.XValid = true
		}
	} else {
		// 4. Append any new entries not already in the log
		rf.AppendNewEntries(args.PrevLogIndex, args.Entries)
		rf.persist()
		// 5. Advance Commit Index for Follower
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.AdvanceCommitIndexFollower(args.LeaderCommit)
		reply.Success = true
		reply.XValid = false
	}
}

func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	// find first logsEntry in Entries that (1) out of range (2) conflicts with Term of rf.logs[same idx]
	for idx, logsEntry := range Entries {
		if prevLogIndex+1+idx > rf.GetLastIndex() || rf.GetLogEntry(prevLogIndex+1+idx).Term != logsEntry.Term {
			rf.logs = append(rf.logs[0:prevLogIndex+1+idx-rf.logStart], Entries[idx:]...)
			break
		}
	}
}

func (rf *Raft) ContainAndMatch(prevLogIndex int, prevLogTerm int) bool {
	if rf.GetFirstIndex() <= prevLogIndex && prevLogIndex <= rf.GetLastIndex() &&
		prevLogTerm == rf.GetLogEntry(prevLogIndex).Term {
		return true
	} else {
		return false
	}
}

func (rf *Raft) AdvanceCommitIndexFollower(LeaderCommit int) {
	if LeaderCommit > rf.GetCommitIndex() {
		newCommitIndex := min(LeaderCommit, rf.GetLastIndex())
		rf.SetCommitIndex(newCommitIndex)
		rf.applyCond.Signal()
	}
}

// *************************************************************************************
// applier

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitIndex = max(rf.commitIndex, rf.logStart)
	rf.lastApplied = max(rf.lastApplied, rf.logStart)
	// we do not apply first log entry, namely {Command:<nil>, Term:0}

	for !rf.killed() {
		// if rf.lastApplied < rf.logStart {
		// 	rf.lastApplied = rf.logStart
		// }

		if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.GetLastIndex() {
			rf.lastApplied++
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
// struct
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) newAEArgs(peer int) AppendEntryArgs {
	next := rf.nextIndex[peer]

	if next < rf.logStart+1 {
		next = rf.logStart + 1
	} else if next > rf.GetLastIndex()+1 {
		next = rf.GetLastIndex() + 1
	}

	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.GetLogEntry(next - 1).Term,
		Entries:      make([]LogEntry, rf.GetLastIndex()-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.GetSubarrayEnd(next))
	return args
}

func (rf *Raft) newAEReply() AppendEntryReply {
	reply := AppendEntryReply{}
	reply.Success = false
	return reply
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
