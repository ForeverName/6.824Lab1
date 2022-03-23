package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
// 这是 raft 必须向服务（或测试人员）公开的 API 大纲。 有关更多详细信息，请参阅以下每个功能的评论。
// rf = Make(...)
//   create a new Raft server. 创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry 开始就新的日志条目达成协议
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader 询问 Raft 的当前任期，以及它是否认为自己是领导者
// ApplyMsg 请求消息
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//  每次向日志提交新条目时，每个 Raft 对等点都应向同一服务器中的服务（或测试者）发送 ApplyMsg。

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//当每个 Raft peer 意识到连续的日志条目被提交时，peer 应该通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试器）发送 ApplyMsg。
//将 CommandValid 设置为 true 以指示 ApplyMsg 包含新提交的日志条目。
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//在实验 3 中，您将希望在 applyCh 上发送其他类型的消息（例如，快照）； 此时，您可以将字段添加到 ApplyMsg，
//但将 CommandValid 设置为 false 以用于这些其他用途。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//定义一个结构来记录追加日志RPC的信息。
type AppendEntries struct {
	Term int // 领导者的任期
	LeaderId int // 领导者Id，因此 follower 可以对客户端进行重定向（因为客户端必须发消息给领导者而不是跟随者）
	PrevLogIndex int // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm int // 紧邻新日志条目之前的那个日志条目的任期
	Entries []Entry // 需要被保存的日志条目（被当作心跳使用时则为空）
	LeaderCommit int// 领导者的已知已提交的最高的日志条目的索引
}

//日志条目信息
type Entry struct {
	Term int
	Command interface{}
	LogIndex int //日志条目索引
}

//追加条目或者心跳通信 的返回值
type AppendEntriesReply struct {
	Term int //当前任期，对于领导者而言，它会更新自己的任期
	Success bool //如果跟随者所含有的条目和PrevLogIndex以及PrevLogTerm匹配上则返回真
}

const (
	Leader int = 1
	Follower int = 2
	Candidate int = 3
)



//
// A Go object implementing a single Raft peer.
// 一个实现单个 Raft peer 的 Go 对象。
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有对等方的 RPC 端点
	persister *Persister          // Object to hold this peer's persisted state 保持此对等点的持久状态的对象
	me        int                 // this peer's index into peers[] 此对等点在peer[]中的索引
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain. 查看论文的图 2，了解 Raft 服务器必须保持什么状态。

	//2A
	//持久性
	currentTerm int //服务器已知最新的任期
	votedFor int  //投给哪个服务器选票，-1的话就表示还没有投给任何服务器
	log []*Entry // 日志条目：每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	role int //表明Raft节点的目前身份

	//易失性
	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	timer time.Time // 选举计时器

	//领导者上的易失性状态
	nextIndex []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

}

// return currentTerm and whether this server
// believes it is the leader.      返回 currentTerm 以及该服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	DPrintf("peer[%d]执行到GetState还没进入到lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer[%d]执行到GetState进入到lock", rf.me)
	term = rf.currentTerm

	if rf.role == Leader {
		isleader = true
	}else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将 Raft 的持久状态保存到稳定的存储中，稍后可以在崩溃和重新启动后检索它。请参阅论文的图 2 以了解应该持久的内容。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.  恢复以前的持久状态。
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure. 示例 RequestVote RPC 参数结构。
// field names must start with capital letters! 字段名称必须以大写字母开头！
//             请求投票的RPC发送内容
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//    2A
	Term int // 候选人的任期号
	CandidateId int // 请求选票的候选人Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.        RequestVote RPC 回复结构示例。
// field names must start with capital letters!    字段名称必须以大写字母开头！
//     返回给候选人的RPC信息
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
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
	DPrintf("peer[%d]向peer[%d]发送选举RPC消息", args.CandidateId, rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("peer[%d]请求peer[%d]投票，peer[%d]的term:=[%d],peer[%d]的term:=[%d]",
			args.CandidateId, rf.me, args.CandidateId,  args.Term, rf.me, rf.currentTerm)
	}else if args.Term == rf.currentTerm {
		//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		//首先要比较哪个日志更新，只有日志更加新的才能有资格成为领导者

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.timer = time.Now()
		}
		DPrintf("peer[%d]请求peer[%d]投票，peer[%d]的rf.votedFor=%d", args.CandidateId, rf.me, rf.me, rf.votedFor)
	} else if args.Term > rf.currentTerm {
		//args.Term > rf.currentTerm 时把rf.votedFor重置为-1 和 rf.role 重置为 Foller
		rf.currentTerm = args.Term
		rf.role = Follower
		//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。这里还没有判断哪个最新，2A暂时不需要
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.timer = time.Now()
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
	//说明是旧领导者发的心跳，忽略即可
	if args.Term < rf.currentTerm {
		DPrintf("peer[%d]收到旧领导者peer[%d]在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	DPrintf("peer[%d]收到peer[%d]发的在term[%d]的心跳消息", rf.me, args.LeaderId, args.Term)
	rf.role = Follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	reply.Success = true
	/*if len(args.Entries) == 0 {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			//rf.role = Follower
			rf.votedFor = -1
		}
		//这个为心跳
		return
	}else {
		//这个为真正的追加日志操作

		//返回假 如果领导者的任期 小于 接收者的当前任期
		if args.Term < rf.currentTerm {
			reply.Success = false
		}else if  false { //暂时这样写
			//在接收者日志中 如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期
			//的日志条目 则继续执行下面的步骤 否则返回假
		}
	}*/

}

//
// example code to send a RequestVote RPC to a server.                     将 RequestVote RPC 发送到服务器的示例代码。
// server is the index of the target server in rf.peers[].                 server 是 rf.peers[] 中目标服务器的索引。
// expects RPC arguments in args.                                          期待args中的RPC争论。
// fills in *reply with RPC reply, so caller should
// pass &reply.															   用 RPC 回复填写 *reply，所以调用者应该通过 &reply。
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the				   传递给 Call() 的参数和回复的类型必须与处理函数中
// handler function (including whether they are pointers).				   声明的参数的类型相同（包括它们是否为指针）。
//
// The labrpc package simulates a lossy network, in which servers          LabRPC包模拟了一个有损网络，其中服务器可能无法访问，
// may be unreachable, and in which requests and replies may be lost.      并且在哪个请求和回复中可能丢失。
// Call() sends a request and waits for a reply. If a reply arrives		   Call() 发送请求并等待回复。 如果回复在超时间隔内到达，
// within a timeout interval, Call() returns true; otherwise			   则 Call() 返回 true； 否则 Call() 返回 false。
// Call() returns false. Thus Call() may not return for a while.           因为 Call() 可能暂时不会返回。
// A false return can be caused by a dead server, a live server that       错误返回可能由死服务器、无法访问的活动服务器、
// can't be reached, a lost request, or a lost reply.					   丢失的请求或丢失的回复引起。
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the  如果服务器端的处理程序函数没有返回。
// handler function on the server side does not return.  Thus there		   Call（）保证返回（可能在延迟之后）*除了*
// is no need to implement your own timeouts around Call().			       因此，无需围绕 Call() 实现您自己的超时。
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've		   如果您无法让 RPC 工作，
// capitalized all field names in structs passed over RPC, and			   请检查您是否已将通过 RPC 传递的结构中的所有字段名称大写，
// that the caller passes the address of the reply struct with &, not	   并且调用者使用 & 传递回复结构的地址，而不是结构本身。
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
//  发送心跳或者日志追加条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start					使用 Raft 的服务（例如 k/v 服务器）
// agreement on the next command to be appended to Raft's log. if this			希望就下一个要附加到 Raft 日志的命令达成一致。
// server isn't the leader, returns false. otherwise start the					如果此服务器不是领导者，则返回 false。
// agreement and return immediately. there is no guarantee that this			否则启动协议并立即返回。 无法保证此命令
// command will ever be committed to the Raft log, since the leader				将永远提交到 Raft 日志，因为领导者可能会
// may fail or lose an election. even if the Raft instance has been killed,		失败或失去选举。 即使 Raft 实例被杀死，
// this function should return gracefully.										这个函数也应该优雅地返回。
//
// the first return value is the index that the command will appear at		第一个返回值是该命令在提交时将出现的索引。
// if it's ever committed. the second return value is the current			第二个返回值是currentTerm。
// term. the third return value is true if this server believes it is		如果此服务器认为它是领导者，则第三个返回值为 true。
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//index =
	term = rf.currentTerm
	isLeader = rf.role == Leader

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,		测试器不会在每次测试后停止由 Raft 创建的 goroutine，
// but it does call the Kill() method. your code can use killed() to		但它会调用 Kill() 方法。您的代码可以使用kill()
// check whether Kill() has been called. the use of atomic avoids the		来检查是否调用了Kill()。atomic的使用避免了对锁的需要。
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew		问题是长时间运行的 goroutine 会占用内存并且可能会
// up CPU time, perhaps causing later tests to fail and generating			占用 CPU 时间，可能会导致以后的测试失败并产生
// confusing debug output. any goroutine with a long-running loop			令人困惑的调试输出。 任何具有长时间运行循环的 goroutine
// should call killed() to check whether it should stop.					都应该调用 kill() 来检查它是否应该停止。
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports		服务或测试人员想要创建一个 Raft 服务器。
// of all the Raft servers (including this one) are in peers[]. this	所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// server's port is peers[me]. all the servers' peers[] arrays			此服务器的端口是 peers[me]。 所有服务器的 peers[] 数组都具有相同的顺序。
// have the same order. persister is a place for this server to			persister 是此服务器保存其持久状态的地方，
// save its persistent state, and also initially holds the most			并且最初还保存最近保存的状态（如果有）。
// recent saved state, if any. applyCh is a channel on which the		applyCh 是测试人员或服务期望 Raft 发送 ApplyMsg 消息的通道。
// tester or service expects Raft to send ApplyMsg messages.			Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//            2A
	rf.role = Follower
	rf.timer = time.Now()
	rf.votedFor = -1
	DPrintf("peer[%d]初始化成功", rf.me)
	go rf.run()
	go rf.AppendEntyiesOrHeartbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//周期性检查是否超时
func (rf *Raft) run() {
	// 如果在随机时间内没有收到其他peer的心跳消息时发送RequestVote RPC 来定期启动领导者选举。
	for !rf.killed() {
		time.Sleep(rf.GetRandSleepTime())
		rf.mu.Lock()
		if time.Since(rf.timer) >= rf.GetRandElection() && rf.role != Leader{
			DPrintf("%v", time.Since(rf.timer))
			//跟随者先要增加自己的当前任期号并且转换到候选人状态。
			rf.currentTerm++
			term := rf.currentTerm
			DPrintf("peer[%d]在term[%d]打算选举成为Leader", rf.me, term)
			rf.role = Candidate
			rf.votedFor = rf.me
			//rf.mu.Unlock()
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
						/*rf.timer = time.Now()
						rf.role = Follower
						if rf.votedFor == rf.me {
							rf.votedFor = -1
						}*/
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
						rf.timer = time.Now()
						rf.mu.Unlock()
						return
					}
					finished++
					cond.Broadcast()
					rf.mu.Unlock()
				}(i)
			}

			//rf.mu.Lock()
			for count < len(rf.peers)/2 + 1 && finished != len(rf.peers){
				cond.Wait()
			}
			DPrintf("peer[%d]在term[%d]获得了%d票,一共返回%d个应答", rf.me, term, count, finished)
			//DPrintf("peer[%d]退出for循环,rf.role=%d,rf.currentTerm=%d,term=%d", rf.me, rf.role, rf.currentTerm, term)
			//这里是为了防止出现多个leader，假设peer[0]在term[1]  sendRequestVote给peer[1]和peer[2],但是peer[0]由于某些原因并没有来得及处理reply
			//导致peer[1]再一次选举超时，peer[1]成为候选者在term[2]sendRequestVote给peer[0]和peer[2],此时term以及又+1，导致peer[1]赢得选举，此时peer[0]
			//又开始处理reply，如果没有这个判断，会导致peer[0]也可以成为领导者。
			if rf.role != Candidate || rf.currentTerm != term {
				DPrintf("rf.role=%d,term=%d,rf.currentTerm=%d", rf.role, term, rf.currentTerm)
				//重新选举
				rf.role = Follower
				rf.votedFor = -1
				rf.timer = time.Now()
				rf.mu.Unlock()
				continue
			}
			if count >= len(rf.peers)/2+1 {
				//说明赢得选举,要向其他服务端发送 RPC表明自己成为了Leader
				rf.role = Leader
				DPrintf("peer[%d]成为了term[%d]领导者", rf.me, rf.currentTerm)
				rf.timer = time.Now()
				//rf.mu.Unlock()
				//启动心跳或者追加日志功能
				//go rf.AppendEntyiesOrHeartbeat()
			} else {
				//选举失败,重置选举时间
				DPrintf("peer[%d] 选举失败", rf.me)
				rf.timer = time.Now()
				if rf.votedFor == rf.me {
					rf.votedFor = -1
				}
				rf.role = Follower
				//rf.mu.Unlock()
			}
		}
		rf.mu.Unlock()
	}
}

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

//获取随机睡眠时间
func (rf *Raft) GetRandSleepTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(50)+ 20) * time.Millisecond
}

//获得随机选举超时时间
func (rf *Raft) GetRandElection() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(200+rand.Int31n(150)) * time.Millisecond
}
