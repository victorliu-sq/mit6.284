package raft

import (
	"bytes"

	"6.824/labgob"
)

// Persist
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.logStartIndex)
	e.Encode(rf.logStartTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.logStartIndex)
	e.Encode(rf.logStartTerm)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// data stores state
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var logStartIndex int
	var logStartTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil || d.Decode(&logStartIndex) != nil || d.Decode(&logStartTerm) != nil {
		Debug(dError, "{Error} During reading Persis")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		rf.logStartIndex = logStartIndex
		rf.logStartTerm = logStartTerm
		Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
	}
}
