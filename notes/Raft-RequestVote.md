# 1. Election Ticker

## ElectionTicker()

while rf is still alive
(1) electionTick
(2) sleep for electionTickTime

```go
const electionTickTime = 30 * time.Microsecond

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		rf.electionTick()
		time.Sleep(electionTickTime)
	}
}
```

## ElectionTick()

(1) if rf is Leader, reset electionTime

(2) else, if election time out, start election

```go
func (rf *Raft) electionTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsLeader() {
		rf.SetElectionTime()
	} else {
		if time.Now().After(rf.electionTime) {
			rf.startElection()
		}
	}
}

// election time out range in [min, max]
func GetRandomElectionTimeout() time.Duration {
	min := 1000
	max := 1300
	return time.Duration((rand.Intn(max-min+1) + min)) * time.Millisecond
}

func (rf *Raft) SetElectionTime() {
	t := time.Now()
	t = t.Add(GetRandomElectionTimeout())
	rf.electionTime = t
}
```

## StartElection()

(1) convert to candidate

(set state to candidate, vote for itself, reset electionTime and  increment current term)

(2) broadcast requestVote

```go
func (rf *Raft) startElection() {
	rf.ConvertToCandidate()
	rf.SetElectionTime()
	rf.BroadcastRequestVote()
}
```



# 2. Broadcast RequestVote

## BroadcastRequestVote()

(1) set votes to 1

(2) send RequestVote to all servers except itself

```go
func (rf *Raft) BroadcastRequestVote() {
	votes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := rf.newRVArgs()
		reply := rf.newRVReply()
		go rf.RequestVoteSender(&votes, peer, &args, &reply)
	}
}
```



# 3. RequestVote Sender

## RequestVoteSender()

(1) send RequestVote to server and receive reply message(RPC)

(2) Rule for all servers:

​	if term of receiver > term of sender, convert to follower and set term

(3) if got voted and votes received from majority of servers:

​	convert to leader and broadcast heartBeats

```go
func (rf *Raft) RequestVoteSender(votes *int, peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if !rf.sendRequestVote(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Rule for all servers
	if reply.Term > rf.GetTerm() {
		rf.ConvertToFollower(reply.Term)
		// rf.SetElectionTime()
	}

	if reply.VoteGranted {
		// If votes received from majority of servers: become Leader
		(*votes)++
		if *votes == rf.GetMajority() {
			rf.ConvertToLeader()
			// Upon election: send heartbeat to each server
			rf.BroadcastAppendEntry()
		}
	}
}
```



# 4. RequestVote Receiver

## RequestVote

(1) Rule for all servers:

​	if term of sender > term of receiver, convert to follower and set term

(2) 

<1> if term of sender < term of receiver, not vote

<2> else if  a && b, vote

a. receiver has not voted or it has voted for the sender

b. sender is at least as update to date as receiver

<3> Otherwise, not vote

```go
// RequestVote Receiver
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers
	if args.Term > rf.GetTerm() {
		rf.ConvertToFollower(args.Term)
	}
	
    
	if args.Term < rf.GetTerm() {
        // not vote
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
	} else if (rf.GetVoteFor() == -1 || rf.GetVoteFor() == args.CandidateId) &&
		rf.checkUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// vote
        reply.Term = rf.GetTerm()
		reply.VoteGranted = true
		rf.SetVoteFor(args.CandidateId)
		rf.SetElectionTime()
		rf.persist()
	} else {
        // not vote
		reply.Term = rf.GetTerm()
		reply.VoteGranted = false
	}
}
```

## checkUpToDate()

Given term and index of last log of sender and receiver, check if sender is at least as up-to-date as receiver:

return true if a || b

a. term of sender's last log> term of receiver's last log

b. term of sender's last log == term of receiver's last log &&

​	length of sender's logs >= length of receiver's logs

```go
func (rf *Raft) checkUpToDate(candidateIndex int, candidateTerm int) bool {
	if (candidateTerm > rf.GetLastLogEntry().Term) || (candidateTerm == rf.GetLastLogEntry().Term && candidateIndex >= rf.GetLastIndex()) {
		return true
	} else {
		return false
	}
}
```

