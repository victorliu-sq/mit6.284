# 1. Save and read state & snapshot

raft state

```go
1 currentTerm
2 voteFor
3 log

```

state machine state

```go
snapshot
```

## Save raft state

```go
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}
```

## Save raft state and state machine(kv server) state

```go
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}
```

## read raft state

```go
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		Debug(dError, "{Error} During reading Persis")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		Debug(dLog, "[S%d] log(Term) becomes: %q", rf.me, rf.GetTermArray())
	}
}
```

## Read state machine(kv server) state



# 2. Insert Persist()

3 persistent states: (1) logs (2) currentTerm (3) voteFor

if any of those states have changed, we need to call persist()

(1) ConvertToFollower

(2) ConvertToCandidate

(3) RV: receiver votes for candidate

(4) AE: leader start(), receiver append new entries



# 3. Insert InstallSnapshot()

