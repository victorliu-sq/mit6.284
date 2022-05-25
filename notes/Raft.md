# helperFunc

## state convertor

1.convert to Leader



2.convert to Candidate



3.convert to Follower



## log entry

getLastLogEntry



getFirstLogEntry



## appendEntry



## requestVote



## 



# Election

go test -run 2A -race >out

## sender

### 1.electionTimer

time out --> startElection()

(1) term + 1, transfer state to candidate, vote for itself, reset electionTimer

(2) send RV to other servers in parallel

<1> RPC --> create request and reply, call(RV, request, reply)

<2> if receiver(of request) grants vote: 

​		votes++

​		 if votes >= majority:  transfer to leader

<3> if sender(of reply, namely receiver of request)'s currentTerm > receiver's currentTerm: 

​		convert back to follower

​		votefor = -1

​		update currentTerm = receiver's currentTerm



### 2.heartBeatTimer: 

time out --> if state == leader, startBroadcast(HeartBeat = true):

send AE to other servers in parallel: 

(1) create HB request and reply

(2) call(AE, request, reply)



## receiver

1.RV

(1) record receiver's term to send back

(2) Reply false if term < currentTerm and return

(3) if sender's term > receiver's term:

​		<1> convert back to follower

​		<2> update currentTerm of receiver

​		<3> votedFor = -1

(4) if	a. votedFor is null or candidateId  

​			b. candidate’s log is at least as up-to-date as receiver’s log

​		<1> voteFor = candidate

​		<2> VoteGranted = true

​		<3> reset ElectionTimer



2.HB

(1)	if sender's term > receiver's term:

​		<1> convert back to follower

​		<2> update receiver's term = sender's term

​		<3> voteFor = -1

(2) 	reset receiver's electionTimer



# Log

go test -run 2B -race >out

## start(command)

start to deal with this command / log entry

### 1. if not leader

set isLeader = false and return

### 2. if leader

(1) add a new log Entry to rf.log

(2) startBroadCast(HeartBeat = false)

(3) update index, term



## sender

### 1. broadcast(AE)

log replication (optimized)





## receiver

1.

