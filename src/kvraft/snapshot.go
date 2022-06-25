package kvraft

import (
	"bytes"

	"6.824/labgob"
)

// Snapshot
func (kv *KVServer) TrySnapshot(index int) {
	if index == 0 || kv.maxraftstate == -1 {
		return
	}

	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		DPrintf("[kv %v] snapshot the state, raft state size is %v", kv.me, kv.persister.RaftStateSize())
		data := kv.EncodeSnapshot()
		go kv.rf.Snapshot(index, data)
	}
}

// Encode 2 maps into Snapshot
func (kv *KVServer) EncodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.maxSeqIds)
	data := w.Bytes()
	return data
}

// Decode Snapshot into 2 maps
func (kv *KVServer) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var map1 map[string]string
	var map2 map[int64]int
	if d.Decode(&map1) != nil || d.Decode(&map2) != nil {
		return
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.state = map1
		kv.maxSeqIds = map2
	}
}
