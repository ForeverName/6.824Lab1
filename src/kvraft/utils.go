package kvraft

import "bytes"
import "../labgob"
//检查请求是否已经执行了
func (kv *KVServer) RepeatCheckL(clientId int64, requestId int) bool {
	lastRequestId, exist := kv.DuplicateDetection[clientId]
	if !exist {
		return false
	}
	if lastRequestId >= requestId {
		//说明这个客户端的请求已经完成了
		return true
	}
	return false
}

// 安装快照
func (kv *KVServer) InstallSnapshot(snapshot []byte, lastIncludedIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	DPrintf("kv[%d]安装快照,lastIncludedIndex=%d", kv.me, lastIncludedIndex)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvdb map[string]string
	var duplicateDetection map[int64]int

	if d.Decode(&kvdb) != nil || d.Decode(&duplicateDetection) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
	} else {
		kv.KVDB = kvdb
		kv.DuplicateDetection = duplicateDetection
	}
	kv.ApplyLogIndex = lastIncludedIndex
	kv.rf.SetLastApplied(lastIncludedIndex)
}

