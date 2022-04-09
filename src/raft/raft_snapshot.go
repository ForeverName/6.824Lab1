package raft

type InstallSnapShotArgs struct {
	Term int	// 当前任期号(currentTerm)
	LeaderId int	// 领导人的ID
	LastIncludedIndex int // 快照中包含的最后日志条目的索引值
	LastIncludedTerm int // 快照中包含的最后日志条目的任期号
	Data []byte
	Done bool // 如果这是最后一个分块则为true
}

type InstallSnapShotReply struct {
	Term int // 当前任期号(currentTerm)，便于领导者更新自己
}


