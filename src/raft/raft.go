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
//  每次向日志提交新条目时，每个 Raft peers 都应向同一服务器中的服务（或测试者）发送 ApplyMsg。

import (
	"../labgob"
	"bytes"
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
	ConflictIndex int //冲突索引，这样便于Leader根据返回的值快速定位冲突的位置，更新nextIndex的值
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
	log []Entry // 日志条目：每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
				// commitIndex - 1为对应切片的下标，因为log第一个索引为1
	// Lab3B
	LastIncludedIndex int // 快照中包含的最后日志条目的索引值
	LastIncludedTerm int // 快照中包含的最后日志条目的任期号

	applyCh chan ApplyMsg
	applyCond *sync.Cond
	role int //表明Raft节点的目前身份

	//易失性
	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	timer time.Time // 选举计时器

	//领导者上的易失性状态
	nextIndex []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）,
					// nextIndex[i]-1为log切片的下标，nextIndex[i]记录的是对应raft peer的下一条lof的LogIndex数值
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
					//这个用于查看matchIndex[i]的值是否大于N，如果有一半的服务器>N，且最后的索引对应的日志的任期是当前Leader的任期
					//则说明日志条目索引N可以提交

}

// return currentTerm and whether this server
// believes it is the leader.      返回 currentTerm 以及该服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//DPrintf("peer[%d]执行到GetState还没进入到lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("peer[%d]执行到GetState进入到lock", rf.me)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	rf.persister.SaveRaftState(w.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil{
		DPrintf("peer[%d]readPersist error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
	}
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
	if rf.killed() {
		return -1, -1 , false
	}
	if rf.role != Leader {
		return -1, -1, false
	}
	if len(rf.log) == 0 {
		//第一个索引为1
		index = 1
	}else {
		index = rf.log[len(rf.log)-1].LogIndex + 1
	}
	term = rf.currentTerm
	DPrintf("追加日志Entry{Term: %d, Command: %v, LogIndex: %d}", term, command, index)
	rf.log = append(rf.log, Entry{Term: term, Command: command, LogIndex: index})
	rf.persist()
	DPrintf("peer[%d]追加完成后的日志条目为%v", rf.me, rf.log)
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
	rf.mu.Lock()
	rf.role = Follower
	rf.setElectionTime()
	rf.votedFor = -1

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("peer[%d]初始化成功,且currentTerm=%d,votedFor=%d,log=%v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	go rf.ElectionTicker()
	//go rf.AppendEntyiesOrHeartbeat()

	return rf
}