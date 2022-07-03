package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/shardctrler"
)

// Snapshot
func (kv *ShardKV) TrySnapshot(index int) {
	if index == 0 || kv.maxraftstate == -1 {
		return
	}

	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		// DPrintf("[kv %v] snapshot the state, raft state size is %v", kv.me, kv.persister.RaftStateSize())
		data := kv.EncodeSnapshot()
		go kv.rf.Snapshot(index, data)
	}
}

// Encode 2 maps into Snapshot
func (kv *ShardKV) EncodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.maxSeqIds)
	e.Encode(kv.config)
	e.Encode(kv.shards)
	e.Encode(kv.comeInShards)
	e.Encode(kv.comeOutShards2state)
	e.Encode(kv.comeInShardsConfigNum)
	data := w.Bytes()
	return data
}

// Decode Snapshot into 2 maps
func (kv *ShardKV) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state map[string]string
	var maxSeqIds map[int64]int
	var config shardctrler.Config
	var shards map[int]bool
	var comeInShards map[int]bool
	var comeOutShards2state map[int]map[int]map[string]string
	var comeInShardsConfigNum int
	if d.Decode(&state) != nil || d.Decode(&maxSeqIds) != nil || d.Decode(&config) != nil ||
		d.Decode(&shards) != nil || d.Decode(&comeInShards) != nil || d.Decode(&comeOutShards2state) != nil ||
		d.Decode(&comeInShardsConfigNum) != nil {
		return
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.state = state
		kv.maxSeqIds = maxSeqIds
		kv.config = config
		kv.shards = shards
		kv.comeInShards = comeInShards
		kv.comeOutShards2state = comeOutShards2state
		kv.comeInShardsConfigNum = comeInShardsConfigNum
	}
}
