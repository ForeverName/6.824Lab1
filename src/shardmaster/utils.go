package shardmaster

//检查请求是否已经执行了
func (sm *ShardMaster) RepeatCheckL(clientId int64, requestId int) bool {
	lastRequestId, exist := sm.DuplicateDetection[clientId]
	if !exist {
		return false
	}
	if lastRequestId >= requestId {
		//说明这个客户端的请求已经完成了
		return true
	}
	return false
}