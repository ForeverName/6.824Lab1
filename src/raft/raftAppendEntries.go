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
	// 2B  rule2 在接收者日志中如果能找到一个和prevLogIndex和prevLogTerm一样的索引和任期的日志条目则继续执行下面的步骤，否则返回假
	if !rf.Rule2(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		return
	}
	//2B rule3 如果一个已经存在的条目和新条目（即刚刚接收到的条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目


}

//追加条目 rule2
func (rf *Raft) Rule2(PrevLogIndex int, PrevLogTerm int) bool {
	for _, value := range rf.log {
		if value.LogIndex == PrevLogIndex && value.Term == PrevLogTerm {
			return true
		}
	}
	return false
}

//追加条目 rule3
func (rf *Raft) Rule3() {

}