package raft

// Separate goroutine for Log Replication
func (rf *Raft) broadcastLogReplication() {
	// log replication for each server
	Debug(dTimer, "[S%d] broadcasts Log Replication\n", rf.me)
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicatorCond[peer].Signal()
	}
}

func (rf *Raft) replicator(peer int) {
	for !rf.killed() {
		rf.replicatorCond[peer].L.Lock()
		for !rf.needReplication(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicatorCond[peer].L.Unlock()
		Debug(dTimer, "[S%d] replicates log entries to [S%d]\n", rf.me, peer)

		// args := rf.newAEArgs(false)
		// reply := rf.newAEReply()
		// rf.sendAppendEntry(peer, &args, &reply)
	}
}

func (rf *Raft) needReplication(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.IsLeader() && rf.matchIndex[peer] < rf.GetLastLogEntry().Index
}
