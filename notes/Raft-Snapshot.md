# 1.Snapshot

## snapshot(index)

(1) if index out of boundary or duplicate cut: return immediately

(2) cut all log entries from rf.start to index - 1

(3) save state and snapshot into persister

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logStart || index >= rf.logStart+len(rf.logs) {
		return
	}
	Debug(dSnap, "[S%v] Snapshots state to index %v", rf.me, index)

	// numTrimmed := (index - 1) - rf.GetFirstIndex() + 1
	numTrimmed := index - rf.GetFirstIndex()
	rf.CutStart(numTrimmed)
	rf.persistStateAndSnapshot(snapshot)
}
```



## CutStart(numTrimmed)

(1) delete log entries from rf.start to index - 1

(2) set rf.start to index

```go
func (rf *Raft) CutStart(numTrimmed int) {
	rf.logs = append([]LogEntry(nil), rf.logs[numTrimmed:]...)
	rf.logStart += numTrimmed
}
```



# 2. InstallSnapshot Sender

## HeartBeat

When sender receives reply showing not contained,

if nextIndex of peer <= rf.start (normally nextIndex of peer >= rf.start + 1),

leader should send InstallSnapshot to peer

```go
if rf.GetNextIndex(peer) <= rf.GetFirstIndex() {
	snap_args := rf.NewInstallSnapshotArgs()
	snap_reply := rf.NewInstallSnapshotReply()
	go rf.InstallSnapshotSender(peer, &snap_args, &snap_reply)
}
```



## Send InstallSnapshot()

(1) send InstallSnapshot to  

(2) if receiver's term > sender's term, convert to follower

(3) update nextIndex and matchIndex

```go
func (rf *Raft) InstallSnapshotSender(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.sendInstallSnapshot(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// all server rule
	if reply.Term > args.Term {
		rf.ConvertToFollower(reply.Term)
	}
	// update nextIndex and matchIndex
	rf.SetNextIndex(peer, args.StartLogIndex+1)
	rf.SetMatchIndex(peer, args.StartLogIndex)
}
```



# 3. InstallSnapShot Receiver

## InstallSnapshot()

(1) if sender's term > receiver's term,

convert to follower and update term



(2) if startLogIndex <= commitIndex, return immediately



(3) if startLogIndex, startLogTerm exists in rf.logs,  only delete log entries before start

​	else,  discard entire log and add startLogIndex and startLogTerm to log



(4) advance commitIndex and lastApplied



(5) write state and snapshot into rf.persister



(6) apply snap message

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// all server rule
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
	}

	// 1. Reply immediately if term < currentTerm
	reply.Term = rf.GetTerm()
	if args.Term < rf.GetTerm() {
		return
	}

	if args.StartLogIndex <= rf.commitIndex {
		return
	}

	Debug(dLog, "[S%v]{Follower}: startLogIndex is %v, startLogTerm is %v", rf.me, args.StartLogIndex, args.StartLogTerm)
	if rf.IsLogExist(args.StartLogIndex, args.StartLogTerm) {
		numTrimmed := args.StartLogIndex - rf.logStart
		rf.CutStart(numTrimmed)
	} else {
		rf.DiscardEntireLog(args.StartLogIndex, args.StartLogTerm)
	}
	rf.commitIndex = args.StartLogIndex
	rf.lastApplied = args.StartLogIndex

	rf.persistStateAndSnapshot(args.Data)

	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.StartLogTerm,
		SnapshotIndex: args.StartLogIndex,
	}
	rf.mu.Lock()
}
```



## IsLogExist(startLogIndex, startLogTerm)

if a && b, return true

a: logStartIndex is not out of range

b: Term of log[logStartIndex] == logStartTerm

```go
func (rf *Raft) IsLogExist(startLogIndex int, startLogTerm int) bool {
	if rf.logStart <= startLogIndex && 
    startLogIndex <= rf.logStart+len(rf.logs)-1 && 
    rf.GetLogEntry(startLogIndex).Term == startLogTerm {
		return true
	} else {
		return false
	}
}
```



## DiscardEntrieLog(startLogIndex, startLogTerm)

(1) discard all log entries and add startlog to log as the only log entry

(2) set rf.logStart = startLogIndex

```go
func (rf *Raft) DiscardEntireLog(startLogIndex int, startLogTerm int) {
	rf.logs = append([]LogEntry(nil), LogEntry{
        Command: nil, 
        Term: startLogTerm, 
        Index: startLogIndex})
	rf.logStart = startLogIndex
}
```

 

