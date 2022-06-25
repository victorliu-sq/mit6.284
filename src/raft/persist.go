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
	e.Encode(rf.logStart)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.logStart)
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
	var logStart int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil || d.Decode(&logStart) != nil {
		Debug(dError, "{Error} During reading Persis")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		rf.logStart = logStart
		Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
	}
}
