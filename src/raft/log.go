package raft

// log Entry
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (rf *Raft) newLogEntry(cmd interface{}) LogEntry {
	return LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
		Index:   len(rf.log),
	}
}

// return true if candidate is at least as up-to-date as rf
func (rf *Raft) checkUpToDate(candidateIndex int, candidateTerm int) bool {
	// Debug(dRV, "[Candidate]: Term:%d, LastIndex:%d [S%d]: Term:%v, LastIndex:%v", candidateTerm, candidateIndex, rf.me, rf.GetLastLogEntry().Term, rf.GetLastLogEntry().Index)
	if (candidateTerm > rf.GetLastLogEntry().Term) || (candidateTerm == rf.GetLastLogEntry().Term && candidateIndex >= rf.GetLastLogEntry().Index) {
		// Debug(dRV, "[S%d] is update to date\n", rf.me)
		return true
	} else {
		// Debug(dRV, "[S%d] is NOT update to date\n", rf.me)
		return false
	}
}

// To avoid situation where length of log < 1
func (rf *Raft) GetLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) GetFirstLogEntry() LogEntry {
	return rf.log[0]
}

func (rf *Raft) AppendLogEntry(logEntry LogEntry) {
	rf.log = append(rf.log, logEntry)
}

func (rf *Raft) GetLogLength() int {
	return len(rf.log)
}

func (rf *Raft) GetIndex(index int) int {
	return index - rf.GetFirstLogEntry().Index
}

func (rf *Raft) GetLogEntry(index int) LogEntry {
	return rf.log[index]
}

func (rf *Raft) GetSubarrayEnd(idx int) []LogEntry {
	return rf.log[idx:]
}

func (rf *Raft) GetXIndex(prevLogIndex int, prevLogTerm int) int {
	XIndex := prevLogIndex
	for XIndex-1 >= 0 && rf.GetLogEntry(XIndex-1).Term == prevLogTerm {
		XIndex--
	}
	return XIndex
}

func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	// find first logEntry in Entries that (1) out of range (2) conflicts with Term of rf.log[same idx]
	rf.log = append(rf.log[0:prevLogIndex+1], Entries...)
	// for idx, logEntry := range Entries {
	// 	if prevLogIndex+1+idx >= len(rf.log) || rf.GetLogEntry(prevLogIndex+1+idx).Term != logEntry.Term {
	// 		rf.log = append(rf.log[0:prevLogIndex+1+idx], Entries[idx:]...)
	// 		break
	// 	}
	// }
}
