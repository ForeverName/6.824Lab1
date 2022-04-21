package shardmaster

import (
	"../raft"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32 // set by Kill()
	waitApplyCh map[int]chan Op // map[logIndex]chan 对于每一个logIndex建立一个对应的通道来通知已经完成
	DuplicateDetection map[int64]int //存储每一个clientId对应的最后一个RequestId，为了防止重复请求

	configs []Config // indexed by config num		由配置编号索引
}


type Op struct {
	// Your data here.
	Operation string
	ClientId int64
	RequestId int
	QueryNum int
	JoinServers map[int][]string
	LeaveGids []int
	MoveShard int
	MoveGid int
}

// JOIN 会给一组GID -> SERVER的映射。其实就是把这些GID 组，加到MASTER的管理范围里来。那么有新的GROUP来了。每台机器可以匀一些SHARD过去
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sm.killed() {
		reply.WrongLeader = true
		return
	}
	if _, b := sm.rf.GetState(); !b {
		//说明不是Leader
		reply.WrongLeader = true
		return
	}
	//先检查这个请求是否已经执行过了
	sm.mu.Lock()
	if sm.RepeatCheckL(args.ClientId, args.RequestId) {
		//true 直接返回数据库中的结果即可
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	//把日志写到raft服务器中
	Log := Op{
		Operation: "Join",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
		JoinServers: args.Servers,
	}
	logIndex, _, _ := sm.rf.Start(Log)
	sm.mu.Lock()
	ChLogIndex, exist := sm.waitApplyCh[logIndex]
	if !exist {
		// DPrintf("kv[%d].waitApplyCh[%d]不存在", kv.me, logIndex)
		sm.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = sm.waitApplyCh[logIndex]
	}
	sm.mu.Unlock()
	//只有应用到raft状态机上才能保存到数据库中并且返回给client结果,所以等待ChLogIndex上传入的信息
	select {
	case <- time.After(time.Millisecond*600):
		// DPrintf("kvserver[%d]接收到了cilent[%d]的get操作:%v超时返回timeout", kv.me, args.ClientId, args)
		reply.WrongLeader = true
	case op := <- ChLogIndex:
		if _, b :=sm.rf.GetState(); !b {
			reply.WrongLeader = true
		}else {
			if op.ClientId == Log.ClientId && op.RequestId == Log.RequestId {
				//这样才能唯一定位一个操作是否已经执行了，防止旧leader同步日志的时候把新leader上logIndex位置上的日志当作这个日志
				sm.mu.Lock()
				for key, value := range args.Servers {
					/*type Config struct {
						Num    int
						Shards [10]int
						Groups map[int][]string
					}*/
					// 实现加入操作，并且重新平衡
				}

				sm.mu.Unlock()
			} else {
				reply.WrongLeader = true
			}
		}
	}

	sm.mu.Lock()
	delete(sm.waitApplyCh, logIndex)
	sm.mu.Unlock()

}
// LEAVE 是给一组GID，表示这组GID的SERVER机器们要走。那么他们管的SHARD又要匀给还没走的GROUP
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}
// MOVE 是指定某个SHARD 归这个GID管
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}
// QUERY就是根据CONFIG NUM来找到对应的CONFIG里的SHARD 规则是如何
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sm.killed() {
		reply.WrongLeader = true
		return
	}
	if _, b := sm.rf.GetState(); !b {
		//说明不是Leader
		reply.WrongLeader = true
		return
	}
	//先检查这个请求是否已经执行过了
	sm.mu.Lock()
	if sm.RepeatCheckL(args.ClientId, args.RequestId) {
		//true 直接返回数据库中的结果即可
		reply.WrongLeader = false
		// 根据args.Num来选择返回值
		if args.Num == -1 || args.Num >= len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs) - 1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	//把日志写到raft服务器中
	Log := Op{
		Operation: "Query",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
		QueryNum: args.Num,
	}
	logIndex, _, _ := sm.rf.Start(Log)
	sm.mu.Lock()
	ChLogIndex, exist := sm.waitApplyCh[logIndex]
	if !exist {
		// DPrintf("kv[%d].waitApplyCh[%d]不存在", kv.me, logIndex)
		sm.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = sm.waitApplyCh[logIndex]
	}
	sm.mu.Unlock()
	//只有应用到raft状态机上才能保存到数据库中并且返回给client结果,所以等待ChLogIndex上传入的信息
	select {
	case <- time.After(time.Millisecond*600):
		// DPrintf("kvserver[%d]接收到了cilent[%d]的get操作:%v超时返回timeout", kv.me, args.ClientId, args)
		reply.WrongLeader = true
	case op := <- ChLogIndex:
		if _, b :=sm.rf.GetState(); !b {
			reply.WrongLeader = true
		}else {
			if op.ClientId == Log.ClientId && op.RequestId == Log.RequestId {
				//这样才能唯一定位一个操作是否已经执行了，防止旧leader同步日志的时候把新leader上logIndex位置上的日志当作这个日志
				sm.mu.Lock()
				if args.Num == -1 || args.Num >= len(sm.configs) {
					reply.Config = sm.configs[len(sm.configs) - 1]
				} else {
					reply.Config = sm.configs[args.Num]
				}
				sm.mu.Unlock()
			} else {
				reply.WrongLeader = true
			}
		}
	}

	sm.mu.Lock()
	delete(sm.waitApplyCh, logIndex)
	sm.mu.Unlock()
}


//
// the tester calls Kill() when a ShardMaster instance won't	当不再需要 ShardMaster 实例时，测试人员调用 Kill()。
// be needed again. you are not required to do anything			您不需要在 Kill() 中执行任何操作，但（例如）
// in Kill(), but it might be convenient to (for example)		关闭此实例的调试输出可能会很方便。
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}
// needed by shardkv tester			shardkv 测试人员需要
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of	servers[] 包含将通过 Paxos 协作以形成容错 shardmaster 服务的一组服务器的端口。
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].		me 是 servers[] 中当前服务器的索引。
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
