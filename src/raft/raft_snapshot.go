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

//落地快照并截断raft的日志
func (rf *Raft) TakeSnapshotAndTruncateTheLog(snapshot []byte , applyLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if applyLogIndex <= rf.LastIncludedIndex {
		return
	}
	compressedLogLength := applyLogIndex - rf.LastIncludedIndex

	rf.LastIncludedIndex = applyLogIndex
	rf.LastIncludedTerm = rf.log[rf.LogIndexToLogArrayIndex(applyLogIndex)].Term

	// 压缩日志
	tempLog := make([]Entry, len(rf.log) - compressedLogLength)
	copy(tempLog, rf.log[compressedLogLength:])
	rf.log = tempLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.RaftStateForPersisit(), snapshot)
}
//节点崩溃恢复时要安装快照
func (rf *Raft) InitInstallSnapshot() {
	applyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot: rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm: rf.LastIncludedTerm,
	}
	rf.lastApplied = rf.LastIncludedIndex
	rf.applyCh <- applyMsg
}

// 处理leader发送给follow的快照
func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		// TODO 写道这了
	}
}

