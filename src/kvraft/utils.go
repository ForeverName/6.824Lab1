package kvraft

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