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

func (rf *Raft) newLogEntry(cmd interface{}) LogEntry {
	return LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
		Index:   rf.logStart + len(rf.logs),
	}
}

// return true if candidate is at least as up-to-date as rf
func (rf *Raft) checkUpToDate(candidateIndex int, candidateTerm int) bool {
	if (candidateTerm > rf.GetLastLogEntry().Term) || (candidateTerm == rf.GetLastLogEntry().Term && candidateIndex >= rf.GetLastIndex()) {
		return true
	} else {
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
	return rf.logs[idx-rf.logStart]
}

func (rf *Raft) GetSubarrayEnd(idx int) []LogEntry {
	return rf.logs[idx-rf.logStart:]
}

func (rf *Raft) GetXIndex(prevLogIndex int, XTerm int) int {
	XIndex := prevLogIndex
	for XIndex-1 >= rf.logStart && rf.GetLogEntry(XIndex-1).Term == XTerm {
		XIndex--
	}
	return XIndex
}
