package raft

import (
	"sync"
	"time"
)

func (rf *Raft) ElectionTicker() {
	for !rf.killed() {
		time.Sleep(rf.GetRandSleepTime())
		rf.mu.Lock()
		if time.Now().After(rf.timer) && rf.role != Leader{
			rf.ElectionL()
		} else {
			rf.mu.Unlock()
		}
		//rf.mu.Unlock()
	}
}

//周期性检查是否超时
func (rf *Raft) ElectionL() {
		DPrintf("%v", time.Since(rf.timer))
		rf.setElectionTime()
		//跟随者先要增加自己的当前任期号并且转换到候选人状态。
		rf.currentTerm++
		term := rf.currentTerm
		DPrintf("peer[%d]在term[%d]打算选举成为Leader", rf.me, term)
		rf.role = Candidate
		rf.votedFor = rf.me
		rf.persist()
		//触发选举,向所有服务器发送 请求选举RPC，首先还要先给自己投一票
		count := 1
		finished := 1
		cond := sync.NewCond(&rf.mu) // 创建一个条件变量
		//rf.mu.Lock()
		peers := rf.peers
		//rf.mu.Unlock()
		for i, _ := range peers {
			if i == rf.me {
				continue
			}
			go func(serverId int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term: term,
					CandidateId: rf.me,
				}
				if len(rf.log) != 0 {
					args.LastLogIndex = rf.log[len(rf.log) - 1].LogIndex
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				//DPrintf("peer[%d]向peer[%d]发送选举RPC消息", rf.me, serverId)
				ok := rf.sendRequestVote(serverId, &args, &reply)
				DPrintf("peer[%d]向peer[%d]在term[%d]发送选举RPC消息后peer[%d]返回的reply为%v:", rf.me, serverId, term, serverId, reply)
				if !ok {
					DPrintf("peer[%d]请求peer[%d]不ok", rf.me, serverId)
				}
				rf.mu.Lock()
				if reply.VoteGranted {
					count++
					DPrintf("peer[%d] 获得了 peer[%d]的选票", rf.me, serverId)
				} else if reply.Term > term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					//说明请求的peer的Term比自身大
					rf.role = Follower
					rf.votedFor = -1
					rf.persist()
					finished++
					cond.Broadcast()
					rf.mu.Unlock()
					return
				}
				finished++
				cond.Broadcast()
				rf.mu.Unlock()
			}(i)
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		defer rf.mu.Unlock()
		for count < len(rf.peers)/2 + 1 && finished != len(rf.peers){
			cond.Wait()
		}
		//DPrintf("peer[%d]在term[%d]获得了%d票,一共返回%d个应答", rf.me, term, count, finished)
		//这里是为了防止出现多个leader，假设peer[0]在term[1]  sendRequestVote给peer[1]和peer[2],但是peer[0]由于某些原因并没有来得及处理reply
		//导致peer[1]再一次选举超时，peer[1]成为候选者在term[2]sendRequestVote给peer[0]和peer[2],此时term以及又+1，导致peer[1]赢得选举，此时peer[0]
		//又开始处理reply，如果没有这个判断，会导致peer[0]也可以成为领导者。
		if rf.role != Candidate || rf.currentTerm != term {
			DPrintf("rf.role=%d,term=%d,rf.currentTerm=%d", rf.role, term, rf.currentTerm)
			//重新选举
			rf.role = Follower
			rf.votedFor = -1
			rf.persist()
			//rf.mu.Unlock()
			return
		}
		if count >= len(rf.peers)/2+1 {
			//说明赢得选举,要向其他服务端发送 RPC表明自己成为了Leader
			rf.role = Leader
			DPrintf("peer[%d]成为了term[%d]领导者", rf.me, rf.currentTerm)
			rf.setElectionTime()
			//初始化自身的nextIndex数组
			rf.InitNextIndexL()
			DPrintf("peer[%d]的nextIndex数组初始化为%v", rf.me, rf.nextIndex)
			//启动心跳或者追加日志功能
			go rf.AppendEntyiesOrHeartbeat()
		} else {
			//选举失败
			DPrintf("peer[%d] 选举失败", rf.me)
			if rf.votedFor == rf.me {
				rf.votedFor = -1
				rf.persist()
			}
			rf.role = Follower
		}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后
	//的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比
	//较长的那个就更加新。

	//如果接收到的 RPC 请求或响应中，任期号 T > currentTerm ，那么就令 currentTerm 等于 T，并
	//切换状态为跟随者
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	//          2A  接收到其他候选人的选举请求
	DPrintf("peer[%d]向peer[%d]发送选举RPC消息:%v", args.CandidateId, rf.me, *args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("peer[%d]请求peer[%d]投票，peer[%d]的term:=[%d],peer[%d]的term:=[%d]",
			args.CandidateId, rf.me, args.CandidateId,  args.Term, rf.me, rf.currentTerm)
	}else if args.Term == rf.currentTerm {
		//首先要比较哪个日志更新，只有日志更加新的才能有资格成为领导者 （候选人的日志至少和自己一样新）2B
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.CompareWhichIsNewerL(args.LastLogIndex, args.LastLogTerm){
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.setElectionTime()
		}
		DPrintf("peer[%d]请求peer[%d]投票，peer[%d]的rf.votedFor=%d", args.CandidateId, rf.me, rf.me, rf.votedFor)
	} else if args.Term > rf.currentTerm {
		//args.Term > rf.currentTerm 时把rf.votedFor重置为-1 和 rf.role 重置为 Follower
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if rf.CompareWhichIsNewerL(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.setElectionTime()
		}
		rf.persist()
	}
}

//比较两个节点的日志哪一个更新,true表示请求方更加新，false表示应答方更加新
func (rf *Raft) CompareWhichIsNewerL(index int, term int) bool {
	//Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后
	//的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比
	//较长的那个就更加新。
	//rf.mu.Lock()
	//defer rf.mu.Unlock() 进入到这个函数之前已经获取锁，所以这里不用再加锁了，否则会造成死锁
	if len(rf.log) == 0 {
		return true
	}
	if term > rf.log[len(rf.log)-1].Term {
		return true
	}else if term == rf.log[len(rf.log)-1].Term {
		//那么日志比较长的那个就更加新。
		if index >= rf.log[len(rf.log)-1].LogIndex {
			return true
		}else {
			return false
		}
	}else {
		return false
	}
}