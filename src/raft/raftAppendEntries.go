package raft

import (
	"time"
)

//领导者周期性的发送心跳或者日志
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
					/*args := AppendEntries{
						Term: term,
						LeaderId: rf.me,

						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()*/
					prevLogIndex := 0
					prevLogTerm := 0
					if len(rf.log) != 0 {
						prevLogIndex = rf.nextIndex[serverId] - 1
						if prevLogIndex != 0 {
							prevLogTerm = rf.log[prevLogIndex - 1].Term
						}
					}
					beginIndex := rf.nextIndex[serverId] - 1
					endIndex := len(rf.log)
					DPrintf("beginIndex=%d,endIndex=%d", beginIndex, endIndex)
					entry := make([]Entry,endIndex - beginIndex)
					copy(entry, rf.log[beginIndex:endIndex])
					DPrintf("peer[%d]给peer[%d]发送消息:%v", rf.me, serverId, entry)
					args := AppendEntries{
						Term: term,
						LeaderId: rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm,
						Entries: entry,
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
					DPrintf("peer[%d]发送peer[%d]的日志后返回结果为为%v",rf.me, serverId, reply)
					if !reply.Success {
						if reply.Term > term {
							//说明是旧leader发的消息
							DPrintf("旧领导peer[%d]发送给peer[%d]的消息现在返回", rf.me, serverId)
							rf.role = Follower
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
							}
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						if reply.ConflictIndex != -1 {
							conflictIndex := reply.ConflictIndex
							rf.nextIndex[serverId] = conflictIndex
							DPrintf("更新peer[%d]的nextIndex[%d]=%d", rf.me, serverId, conflictIndex)
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						return
					}else {
						//reply.Success==true
						rf.nextIndex[serverId] = endIndex + 1
						rf.matchIndex[serverId] = endIndex
						DPrintf("更新peer[%d]的nextIndex[%d]=%d,matchIndex[%d]=%d", rf.me, serverId, rf.nextIndex[serverId], serverId, rf.matchIndex[serverId])
						rf.CheckMatchIndexL(endIndex)
					}
					rf.mu.Unlock()
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(50*time.Millisecond)
	}

}

//把日志应用到状态机上,然后返回给前端，也就是往applyCh传递
func (rf *Raft) AppendLogsL() {
	//首先判断是否有新增的的待提交的日志
	if rf.lastApplied < rf.commitIndex {
		Messages := make([]ApplyMsg,0)

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied +=1
			Messages = append(Messages,ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command: rf.log[rf.lastApplied - 1].Command,
			})
			DPrintf("日志索引%d被应用到状态机peer[%d]上", rf.lastApplied, rf.me)
		}
		for _,messages := range Messages{
			rf.applyCh<-messages
		}
	}
}

// 处理 心跳或追加日志 的RPC handler
func (rf *Raft) AppendEntriesHandler(args *AppendEntries, reply *AppendEntriesReply) {
	//Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后
	//的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比
	//较长的那个就更加新。

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer[%d]接收到peer[%d]的消息为:%v", rf.me, args.LeaderId, args)
	rf.timer = time.Now()
	reply.ConflictIndex = -1
	//说明是旧领导者发的，忽略即可  rule1
	if args.Term < rf.currentTerm {
		//DPrintf("peer[%d]收到旧领导者peer[%d]在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2B  rule2Andrule3 在接收者日志中如果能找到一个和prevLogIndex和prevLogTerm一样的索引和任期的日志条目则继续执行下面的步骤，否则返回假
	//大致意思就是根据prevLogIndex和prevLogTerm看在prevLogIndex索引上的日志Term是否是prevLogTerm，
	//如果是那么把日志追加在prevLogIndex+1到最后，把raft peer(Follower)的原来冲突日志覆盖即可
	//如果不是则返回冲突的索引，并且让Leader对应nextIndex的值为ConflictIndex
	b, conflictIndex := rf.Rule2AndRule3L(args.PrevLogIndex, args.PrevLogTerm)
	if !b {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		return
	}

	//说明是心跳,心跳也需要检查规则rf.Rule2AndRule3L(),以免一个旧领导者(现在是follower,日志条目比新领导者多，但是很多没有commit)
	//接收到新领导的心跳导致rf.log[len(rf.log)-1].LogIndex > PrevLogIndex,但是rf.log[PrevLogIndex-1].Term != PrevLogTerm
	//而把rf.commitIndex更新，实际上旧领导者还没有接收到新领导的日志，会导致旧领导者的旧日志被提交而引发日志不一致的错误.
	if len(args.Entries) == 0 {
		//DPrintf("peer[%d]收到peer[%d]发的在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
		rf.role = Follower
		rf.updateCommitIndexL(args.LeaderCommit)
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = -1
			rf.persist()
		}
		reply.Success = true
		return
	}

	//追加新的日志条目
	DPrintf("peer[%d]原来的日志条目为%v,追加的日志条目为%v", rf.me, rf.log, args.Entries)
	//去除重复的才能添加到日志里面
	rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	rf.persist()
	DPrintf("peer[%d]追加完成后的日志条目为%v", rf.me, rf.log)
	//更新接收者raft的commitIndex
	rf.updateCommitIndexL(args.LeaderCommit)
	reply.Success = true

}

//追加条目 rule2 和 rule3
func (rf *Raft) Rule2AndRule3L(PrevLogIndex int, PrevLogTerm int) (bool, int) {
	//PrevLogIndex-1才是PrevLogIndex对应日志的下标
	//还要考虑接收者没有那么多的日志的一种情况,比如下面的S3为Leader，下一个term为6，
	//一开始prevLogIndex=12 prevLogTerm=5  nextIndex[1]=13 nextIndex[2]=13
	//S2的索引12处的日志不匹配，所以针对S2，prevLogIndex=11 prevLogTerm=3 nextIndex[2]=12，继续发送RPC
	//针对S1，日志没有那么长，所以prevLogIndex=10 prevLogTerm=3 nextIndex[1]=11
	/*	10    11    12    13
	S1  3
	S2  3     3     4
	S3	3     3     5     6*/
	DPrintf("peer[%d]的log为%v", rf.me, rf.log)
	conflictIndex := -1
	//说明是第一条日志
	if PrevLogIndex == 0 {
		return true, conflictIndex
	}
	DPrintf("peer[%d]:rf.log[len(rf.log)-1].LogIndex=%d,PrevLogIndex=%d",
		rf.me, rf.log[len(rf.log)-1].LogIndex, PrevLogIndex)
	if rf.log[len(rf.log)-1].LogIndex < PrevLogIndex {
		conflictIndex = rf.log[len(rf.log)-1].LogIndex + 1 //11,对应S1
		return false, conflictIndex
	}
	DPrintf("peer[%d]:rf.log[PrevLogIndex-1].Term=%d,PrevLogTerm=%d", rf.me, rf.log[PrevLogIndex-1].Term, PrevLogTerm)
	if rf.log[PrevLogIndex-1].Term == PrevLogTerm {
		return true, conflictIndex
	}
	conflictIndex = PrevLogIndex
	return false, conflictIndex//12,对应S2
}

func (rf *Raft) updateCommitIndexL(leaderCommitIndex int) {
	DPrintf("leaderCommitIndex=%d,原来peer[%d].commitIndex=%d", leaderCommitIndex, rf.me, rf.commitIndex)
	//还有一种情况，当follow还没接收到日志，而心跳RPC已经到了，如果不做处理，会导致下标rf.log[len(rf.log) - 1]越界
	if len(rf.log) == 0 {
		return
	}
	if leaderCommitIndex > rf.commitIndex {
		if leaderCommitIndex > rf.log[len(rf.log) - 1].LogIndex {
			rf.commitIndex = rf.log[len(rf.log) - 1].LogIndex
		}else {
			rf.commitIndex = leaderCommitIndex
		}
		DPrintf("peer[%d]更新commitIndex=%d", rf.me, rf.commitIndex)
		//更新rf.commitIndex之后要更新rf.lastApplied
		rf.AppendLogsL()
	}
}

//检查matchIndex数组
func (rf *Raft) CheckMatchIndexL(index int) {
	if rf.commitIndex >= index {
		return
	}
	count := 1
	for _, matchVal := range rf.matchIndex {
		if matchVal >= index {
			count++
		}
	}
	if count >= len(rf.peers)/2+1 {
		DPrintf("Leader=peer[%d]更新commitIndex为%d", rf.me, index)
		rf.commitIndex = index
		rf.AppendLogsL()
	}
}