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

	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	// bytes := []byte{}
	e.Encode(kv.state)
	// DPrintf("-----------------------------------------------------------------")
	// DPrintf("size of kv.state is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.maxSeqIds)
	// DPrintf("size of kv.maxSeqIds is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.config)
	// DPrintf("size of kv.config is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.shards)
	// DPrintf("size of kv.shards is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.comeInShards)
	// DPrintf("size of kv.comeInShards is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.comeInShardsConfigNum)

	e.Encode(kv.comeOutShards2state)
	// DPrintf("size of kv.comeOutShards2state is %v", len(w.Bytes())-len(bytes))
	// bytes = w.Bytes()
	e.Encode(kv.garbageShards)
	// DPrintf("size of kv.garbageShards is %v", len(w.Bytes())-len(bytes))
	data := w.Bytes()
	// DPrintf("size of snapshot is %v", len(data))
	// DPrintf("-----------------------------------------------------------------")
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
	var comeInShardsConfigNum int
	var comeOutShards2state map[int]map[int]map[string]string
	var garbageShards map[int]int
	if d.Decode(&state) != nil || d.Decode(&maxSeqIds) != nil || d.Decode(&config) != nil ||
		d.Decode(&shards) != nil || d.Decode(&comeInShards) != nil || d.Decode(&comeInShardsConfigNum) != nil ||
		d.Decode(&comeOutShards2state) != nil || d.Decode(&garbageShards) != nil {
		return
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.state = state
		kv.maxSeqIds = maxSeqIds
		kv.config = config
		kv.shards = shards
		kv.comeInShards = comeInShards
		kv.comeInShardsConfigNum = comeInShardsConfigNum
		kv.comeOutShards2state = comeOutShards2state
		kv.garbageShards = garbageShards
	}
}
