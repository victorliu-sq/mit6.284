package raft

// logs Entry
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (rf *Raft) CutStart(numTrimmed int) {
	// cut nums up to index
	Debug(dSnap, "length of log is %v, new start is %v", len(rf.logs), numTrimmed)
	rf.logs = append([]LogEntry(nil), rf.logs[numTrimmed:]...)
	rf.logStart += numTrimmed
}

func (rf *Raft) GetFirstIndex() int {
	return rf.logStart
}

func (rf *Raft) GetLastIndex() int {
	return rf.logStart + len(rf.logs) - 1
}

// func (rf *Raft) CutStart(idx int) {
// 	rf.logStart += idx
// 	rf.logs = rf.logs[idx:]
// }

func (rf *Raft) newLogEntry(cmd interface{}) LogEntry {
	return LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
		Index:   rf.logStart + len(rf.logs),
	}
}

// return true if candidate is at least as up-to-date as rf
func (rf *Raft) checkUpToDate(candidateIndex int, candidateTerm int) bool {
	// Debug(dRV, "[Candidate]: Term:%d, LastIndex:%d [S%d]: Term:%v, LastIndex:%v", candidateTerm, candidateIndex, rf.me, rf.GetLastLogEntry().Term, rf.GetLastIndex())
	if (candidateTerm > rf.GetLastLogEntry().Term) || (candidateTerm == rf.GetLastLogEntry().Term && candidateIndex >= rf.GetLastIndex()) {
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

func (rf *Raft) GetLogEntry(idx int) LogEntry {
	// Debug(dSnap, "The index is %v, range is [%v, %v]", idx-rf.logStart, 0, len(rf.logs)-1)
	return rf.logs[idx-rf.logStart]
}

func (rf *Raft) GetSubarrayEnd(idx int) []LogEntry {
	// Debug(dLog, "[S%v]'s log length is %v", rf.me, len(rf.logs))
	// Debug(dLog, "[S%v]'s last Index is %v", rf.me, rf.GetLastIndex())
	return rf.logs[idx-rf.logStart:]
}

func (rf *Raft) GetXIndex(prevLogIndex int, XTerm int) int {
	XIndex := prevLogIndex
	for XIndex-1 >= rf.logStart && rf.GetLogEntry(XIndex-1).Term == XTerm {
		XIndex--
	}
	return XIndex
}

func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	// find first logsEntry in Entries that (1) out of range (2) conflicts with Term of rf.logs[same idx]
	// rf.logs = append(rf.logs[0:prevLogIndex+1], Entries...)
	for idx, logsEntry := range Entries {
		if prevLogIndex+1+idx > rf.GetLastIndex() || rf.GetLogEntry(prevLogIndex+1+idx).Term != logsEntry.Term {
			rf.logs = append(rf.logs[0:prevLogIndex+1+idx-rf.logStart], Entries[idx:]...)
			break
		}
	}
}

func (rf *Raft) IsLogExist(startLogIndex int, startLogTerm int) bool {
	if rf.logStart <= startLogIndex && startLogIndex <= rf.logStart+len(rf.logs)-1 && rf.GetLogEntry(startLogIndex).Term == startLogTerm {
		return true
	} else {
		return false
	}
}

func (rf *Raft) DiscardEntireLog(lastIncludedIndex int, lastIncludedTerm int) {
	rf.logs = append([]LogEntry(nil), LogEntry{Command: nil, Term: lastIncludedTerm, Index: lastIncludedIndex})
	rf.logStart = lastIncludedIndex
}
