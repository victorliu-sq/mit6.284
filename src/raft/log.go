package raft

// log Entry
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func (rf *Raft) newLogEntry(cmd interface{}) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
		Index:   len(rf.log),
	}
}

// return true if 1 is at least as up-to-date as 2
func checkUpToDate(logIndex1 int, logTerm1 int, logIndex2 int, logTerm2 int) bool {
	if (logTerm1 > logTerm2) || (logTerm1 == logTerm2 && logIndex1 >= logIndex2) {
		return true
	}
	return false
}

// To avoid situation where length of log < 1
func (rf *Raft) GetLastLogEntry() LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return LogEntry{
			Index: 0,
			Term:  0,
		}
	}
}

func (rf *Raft) GetFirstLogEntry() LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.log) > 0 {
		return rf.log[0]
	} else {
		return LogEntry{
			Index: 0,
			Term:  0,
		}
	}
}

func (rf *Raft) AppendLogEntry(logEntry LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, logEntry)
}

func (rf *Raft) GetLogLength() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.log)
}

func (rf *Raft) GetIndex(index int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return index - rf.GetFirstLogEntry().Index
}

func (rf *Raft) GetLogEntry(index int) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// Debug(dLog, "index is %d\n", index)
	return rf.log[index]
}

func (rf *Raft) GetSubarrayEnd(idx int) []LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.log[idx:]
}

func (rf *Raft) GetXIndex(prevLogIndex int, prevLogTerm int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	XIndex := prevLogIndex
	for XIndex-1 >= 0 && rf.GetLogEntry(XIndex-1).Term == prevLogTerm {
		XIndex--
	}
	return XIndex
}

func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	// find first logEntry in Entries that (1) out of range (2) conflicts with Term of rf.log[same idx]
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log[0:prevLogIndex+1], Entries...)
	// for idx, logEntry := range Entries {
	// 	if idx >= len(rf.log) || rf.GetLogEntry(idx).Term != logEntry.Term {
	// 		rf.log = append(rf.log[0:prevLogIndex+1+idx], Entries[idx:]...)
	// 		break
	// 	}
	// }
}
