package raft

// logs Entry
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (rf *Raft) newLogEntry(cmd interface{}) LogEntry {
	return LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
		Index:   len(rf.logs),
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

// To avoid situation where length of logs < 1
func (rf *Raft) GetLastLogEntry() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) GetFirstLogEntry() LogEntry {
	return rf.logs[0]
}

func (rf *Raft) AppendLogEntry(logsEntry LogEntry) {
	rf.logs = append(rf.logs, logsEntry)
}

func (rf *Raft) GetLogLength() int {
	return len(rf.logs)
}

func (rf *Raft) GetIndex(index int) int {
	return index - rf.GetFirstLogEntry().Index
}

func (rf *Raft) GetLogEntry(index int) LogEntry {
	return rf.logs[index]
}

func (rf *Raft) GetSubarrayEnd(idx int) []LogEntry {
	return rf.logs[idx:]
}

func (rf *Raft) CutToEnd(idx int) {
	rf.logs = rf.logs[idx:]
}

func (rf *Raft) GetXIndex(prevLogIndex int, prevLogTerm int) int {
	XIndex := prevLogIndex
	for XIndex-1 >= 0 && rf.GetLogEntry(XIndex-1).Term == prevLogTerm {
		XIndex--
	}
	return XIndex
}

func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	// find first logsEntry in Entries that (1) out of range (2) conflicts with Term of rf.logs[same idx]
	rf.logs = append(rf.logs[0:prevLogIndex+1], Entries...)
	for idx, logsEntry := range Entries {
		if prevLogIndex+1+idx >= len(rf.logs) || rf.GetLogEntry(prevLogIndex+1+idx).Term != logsEntry.Term {
			rf.logs = append(rf.logs[0:prevLogIndex+1+idx], Entries[idx:]...)
			break
		}
	}
}
