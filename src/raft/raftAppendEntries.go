package raft

import "time"

//领导者周期性的发送心跳或者追加条目
func (rf *Raft) AppendEntyiesOrHeartbeat() {
	//   2A 目前先不追加条目，只是发送心跳
	for !rf.killed() {
		//time.Sleep(100*time.Millisecond)
		rf.mu.Lock()
		state := rf.role
		//rf.mu.Unlock()
		if state == Leader {
			//rf.mu.Lock()
			peers := rf.peers
			term := rf.currentTerm
			//rf.mu.Unlock()
			for i, _ := range peers {
				//rf.mu.Lock()
				if rf.role != Leader {
					//rf.mu.Unlock()
					break
				}
				if i == rf.me {
					//rf.mu.Unlock()
					continue
				}
				//rf.mu.Unlock()
				go func(serverId int) {
					rf.mu.Lock()
					args := AppendEntries{
						Term: term,
						LeaderId: rf.me,

						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(serverId, &args, &reply)
					//处理reply
					if !ok {
						return
					}
					rf.mu.Lock()
					rf.timer = time.Now()
					/*if rf.role != Leader {
						rf.mu.Unlock()
						return
					}*/
					if !reply.Success {
						if reply.Term > term {
							//说明是旧leader发的消息
							DPrintf("旧领导peer[%d]发送给peer[%d]的消息现在返回", rf.me, serverId)
							rf.role = Follower
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
							}
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}
					}else {
						//reply.Success==true

					}
					rf.mu.Unlock()
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100*time.Millisecond)
	}

}

//把日志应用到状态机上,然后返回给前端，也就是往applyCh传递
func (rf *Raft) AppendLogs() {
	//首先判断是否有新增的的待提交的日志
	if rf.lastApplied < rf.commitIndex {

	}
}

// 处理 心跳或追加日志 的RPC handler
func (rf *Raft) AppendEntriesHandler(args *AppendEntries, reply *AppendEntriesReply) {
	//Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后
	//的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比
	//较长的那个就更加新。

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer = time.Now()
	//说明是旧领导者发的心跳，忽略即可  rule1
	if args.Term < rf.currentTerm {
		//DPrintf("peer[%d]收到旧领导者peer[%d]在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//说明是心跳
	if len(args.Entries) == 0 {
		//DPrintf("peer[%d]收到peer[%d]发的在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
		rf.role = Follower
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = -1
		}
		return
	}
	// 2B  rule2Andrule3 在接收者日志中如果能找到一个和prevLogIndex和prevLogTerm一样的索引和任期的日志条目则继续执行下面的步骤，否则返回假
	//大致意思就是根据prevLogIndex和prevLogTerm看在prevLogIndex索引上的日志Term是否是prevLogTerm，
	//如果是那么把日志追加在prevLogIndex+1到最后，把raft peer(Follower)的原来冲突日志覆盖即可
	//如果不是则返回冲突的索引，并且让Leader对应nextIndex的值-1
	b, conflictIndex := rf.Rule2AndRule3L(args.PrevLogIndex, args.PrevLogTerm)
	if !b {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		return
	}

	//追加新的日志条目
	rf.log = append(rf.log, args.Entries...)

	//更新接收者raft的commitIndex
	rf.updateCommitIndexL(args.LeaderCommit)
	reply.Success = true
}

//追加条目 rule2 和 rule3
func (rf *Raft) Rule2AndRule3L(PrevLogIndex int, PrevLogTerm int) (bool, int) {
	/*for _, value := range rf.log {
		if value.LogIndex == PrevLogIndex && value.Term == PrevLogTerm {
			return true
		}
	}*/
	//PrevLogIndex-1才是PrevLogIndex对应日志的下标
	//还要考虑接收者没有那么多的日志的一种情况,比如下面的S3为Leader，下一个term为6，
	//一开始prevLogIndex=12 prevLogTerm=4  nextIndex[1]=13 nextIndex[2]=13
	//S2的索引12处的日志不匹配，所以针对S2，prevLogIndex=11 prevLogTerm=3 nextIndex[2]=12，继续发送RPC
	//针对S1，日志没有那么长，所以prevLogIndex=10 prevLogTerm=3 nextIndex[2]=11
	/*	10    11    12    13
	S1  3
	S2  3     3     4
	S3	3     3     5     6*/
	conflictIndex := -1
	if rf.log[len(rf.log)-1].LogIndex < PrevLogIndex {
		conflictIndex = rf.log[len(rf.log)-1].LogIndex + 1 //11
		return false, conflictIndex
	}
	if rf.log[PrevLogIndex-1].Term == PrevLogTerm {
		return true, conflictIndex
	}
	conflictIndex = PrevLogIndex
	return false, conflictIndex//12
}

func (rf *Raft) updateCommitIndexL(leaderCommitIndex int) {
	if leaderCommitIndex > rf.commitIndex {
		if leaderCommitIndex > rf.log[len(rf.log) - 1].LogIndex {
			rf.commitIndex = rf.log[len(rf.log) - 1].LogIndex
		}else {
			rf.commitIndex = leaderCommitIndex
		}
	}
}