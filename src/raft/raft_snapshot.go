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
	DPrintf("peer[%d]收到生成快照请求rf.LastIncludedIndex=%d, applyLogIndex=%d", rf.me, rf.LastIncludedIndex, applyLogIndex)
	if applyLogIndex <= rf.LastIncludedIndex {
		return
	}
	compressedLogLength := applyLogIndex - rf.LastIncludedIndex

	rf.LastIncludedTerm = rf.log[rf.LogIndexToLogArrayIndex(applyLogIndex)].Term
	rf.LastIncludedIndex = applyLogIndex

	// 压缩日志
	tempLog := make([]Entry, len(rf.log) - compressedLogLength)
	copy(tempLog, rf.log[compressedLogLength:])
	rf.log = tempLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.RaftStateForPersisit(), snapshot)
}
//安装快照到应用层
func (rf *Raft) InstallSnapshotToApplication() {
	applyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot: rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm: rf.LastIncludedTerm,
	}
	if rf.lastApplied < rf.LastIncludedIndex {
		rf.lastApplied = rf.LastIncludedIndex
	}
	rf.applyCh <- applyMsg
	DPrintf("peer[%d]崩溃恢复完成", rf.me)
}

// 处理leader发送给follow的快照
func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	rf.setElectionTime()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}
	// 如果leader发送的快照还没有follow的本地快照长，则直接返回
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	} else { // leader快照比本地长
		rf.log = make([]Entry, 0)
	}
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm

	rf.persister.SaveStateAndSnapshot(rf.RaftStateForPersisit(), args.Data)
	rf.InstallSnapshotToApplication()
}

