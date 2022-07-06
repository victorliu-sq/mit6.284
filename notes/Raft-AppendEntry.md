# 1. HeartBeat Ticker

if nextIndex[peer] <= lastIndex, the ticker will send log replication message automatically instead of heartBeat message until the follower has been udpated

## heartBeatTicker()

while rf is still alive:

(1) heartBeat Tick

(2) sleep for heartBeat Tick Time

```go
func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		rf.heartBeatTick()
		time.Sleep(heartBeatTickTime)
	}
}
```

## heartBeatTick()

if raft is leader: 

(1) reset electionTime 

(2) broadcast AppendEntry for heartBeat or log replication

```go
func (rf *Raft) heartBeatTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
		rf.BroadcastAppendEntry(true)
	}
}
```



# 2. Append Command

## Start()

(1) if server is not leader, return false immediately

(2) Add a new log entry containing the command to log of server

(3) broadcast AppendEntry for log replication

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. if rf is not leader: return false
	if !rf.IsLeader() {
		return -1, rf.GetTerm(), false
	}

	// 2. Add a new logEntry to rf.log
	logEntry := rf.newLogEntry(command)
	rf.AppendLogEntry(logEntry)
	index, term := logEntry.Index, logEntry.Term
	rf.persist()
    // 3. broadcast AppendEntry to each server
	rf.BroadcastAppendEntry(false)

	return index, term, true
}
```



# 3. BroadCast AppendEntry

## BroadcastAppendEntry()

send AppendEntry to each peer server if a || b

a. isHeartBeat == true

b. NextIndex[peer] <= last Index of sender

```go
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
```



# 4. AppendEntry Sender

## AppendEntrySender()

(1) Send AppendEntry to server and receive reply message

(2) Rule for all servers:

​	if term of receiver > term of sender, convert to follower and set term

(3) Process reply

(4) Advance Commit Index 

(5) Apply all committed log entries to state machine

```go
func (rf *Raft) AppendEntrySender(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.sendAppendEntry(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all server
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		return
	}

	// Process Reply
	rf.ProcessReply(peer, args, reply)

	// Advance commit Index of leader
	rf.AdvanceCommitIndexLeader()
    
    // Apply newly committed log entries 
	rf.applyCond.Signal()
}
```



## ProcessReply()

(1) Since server can be advanced during RPC, we need to check whether it has been updated

if a || b, return immediately

a: server is no longer leader

b: server's term has been out of date



(2) 

<1> 

if sender's prevLogIndex and prevLogTerm can match receiver's log,

**try to** update nextIndex of peer with prev + len(entries) + 1

**try to** update matchIndex of peer with prevLogIndex + len(entries)



<2>

if receiver contains prevLogIndex but mismatches prevLogTerm,

**try to** update nextIndex of peer with first index of mismatched term



<3> 

Otherwise,  namely if receiver does not contain prevLogIndex or heartBeat Message,

**try to** update nextIndex of peer with  len(receiver's log)



<1> tries to make nextIndex and matchIndex bigger

<2><3> try to make nextIndex smaller

```go
func (rf *Raft) ProcessReply(peer int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if !rf.IsLeader() || rf.GetTerm() != args.Term {
		return
	}

	if reply.Success {
		// contain and match
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
		if newNext < oldNext {
			rf.SetNextIndex(peer, newNext)
		}
	} else {
		// not contain
		newNext := reply.XIndex
		if newNext < rf.GetNextIndex(peer) {
			rf.SetNextIndex(peer, newNext)
		}
	}
}
```





## AdvanceCommitIndexLeader()

(1) if server is no longer leader, return immediately

(2) choose N from commitIndex + 1 to index of last log entry

(3) if majority of matchIndex[i] >= N && log[N].term == current term,

​	set commitIndex to N

```go
func (rf *Raft) AdvanceCommitIndexLeader() {
	if !rf.IsLeader() {
		return
	}

	for N := rf.GetCommitIndex() + 1; N <= rf.GetLastIndex(); N++ {
		if rf.GetLogEntry(N).Term == rf.GetTerm() {
			num := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.GetMatchIndex(peer) >= N {
					num++
					if num == rf.GetMajority() {
						rf.SetCommitIndex(N)
						break
					}
				}
			}
		}
	}
}
```



## Apply

```go
rf.applyCond.Signal()
```



# 5. AppendEntry Receiver

## AppendEntry

(1) Rule for all server

(2) reset election timer (log replication message must be a heartBeat message)

(3) Reply false if term of sender < term of receiver

(4) 

<1> if ! contain prevLogIndex and match prevLogTerm

[1] if not contain prevLogIndex

set XValid to false

set XIndex to length of receiver's log



[2] if contain prevLogIndex but mismatch

set XValid to true

set XIndex to first index of **XTerm**

set XTerm to **Term of log[prevLogIndex]**



<2> otherwise (contain and match)

set success to true

append entries following prevLogIndex to log

advance commitIndex of follower

apply

```go
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
			Debug(dTerm, "[S%d] becomes {Follower}", rf.me)
		}
		if args.Term > term {
			Debug(dTerm, "[S%d] currentTerm -> (%d)", rf.me, rf.GetTerm())
		}
	}

	// 2.Rely false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.ContainAndMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex > rf.GetLastIndex() || args.PrevLogIndex < rf.GetFirstIndex() {
			// not contain --> return length of logs
			reply.XValid = false
			reply.XIndex = rf.GetLastIndex() + 1
		} else {
			// contain but mismatch --> conflict
			reply.XValid = true
			reply.XTerm = rf.GetLogEntry(args.PrevLogIndex).Term
			reply.XIndex = rf.GetXIndex(args.PrevLogIndex, reply.XTerm)
		}
	} else {
        // contain and match
		reply.Success = true
		reply.XValid = false
        // Append any new entries not already in the log
		rf.AppendNewEntries(args.PrevLogIndex, args.Entries)
		rf.persist()
		// Advance Commit Index for Follower
		rf.AdvanceCommitIndexFollower(args.LeaderCommit)
        // Apply
		rf.applyCond.Signal()
	}
}
```



## ContainAndMatch

return true if prevLogIndex is in log and prevLogTerm can match

```go
func (rf *Raft) ContainAndMatch(prevLogIndex int, prevLogTerm int) bool {
	if rf.GetFirstIndex() <= prevLogIndex && prevLogIndex <= rf.GetLastIndex() &&
		prevLogTerm == rf.GetLogEntry(prevLogIndex).Term {
		return true
	} else {
		return false
	}
}
```

## AppendNewEntries

find first logsEntry in Entries that  a || b

a. out of range 

b. conflicts with Term of rf.logs[same idx]

```go
func (rf *Raft) AppendNewEntries(prevLogIndex int, Entries []LogEntry) {
	for idx, logsEntry := range Entries {
		if prevLogIndex+1+idx > rf.GetLastIndex() || rf.GetLogEntry(prevLogIndex+1+idx).Term != logsEntry.Term {
			rf.logs = append(rf.logs[0:prevLogIndex+1+idx-rf.logStart], Entries[idx:]...)
			break
		}
	}
}
```



## AdvanceCommitIndexFollower()

if commit Index of leader > receiver's commit Index

set commitIndex of follower to min(LeaderCommitIndex, Last Index of receiver)

```go
func (rf *Raft) AdvanceCommitIndexFollower(LeaderCommit int) {
	if LeaderCommit > rf.GetCommitIndex() {
		newCommitIndex := min(LeaderCommit, rf.GetLastIndex())
		rf.SetCommitIndex(newCommitIndex)
	}
}
```



## Apply 

```go
rf.applyCond.Signal()
```



# 6. Applier

## applier()

(1) initialize lastApplied

(2) while rf has not been killed

<1> if there are still committed log entries left to apply, 

lastApplied++

apply new log entry to apply channel

<2> else,

applyCond.Wait()

```go
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = 0
	// we do not apply first log entry, namely {Command:<nil>, Term:0}
	if rf.lastApplied < rf.logStart {
		rf.lastApplied = rf.logStart
	}

	for !rf.killed() {
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
```

Some design:

(1) `unlock()` before apply into channel and `lock()` again

(2) `wait()` will unlock automatically