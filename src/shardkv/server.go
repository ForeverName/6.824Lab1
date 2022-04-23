package shardkv


// import "../shardmaster"
import (
	"../labrpc"
	"sync/atomic"
	"time"
)
import "../raft"
import "sync"
import "../labgob"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	ClientId int64
	RequestId int
	Key string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead    int32 // set by Kill()

	KVDB map[string]string //模拟一个数据库，数据都存在map里面
	waitApplyCh map[int]chan Op // map[logIndex]chan 对于每一个logIndex建立一个对应的通道来通知已经完成
	DuplicateDetection map[int64]int //存储每一个clientId对应的最后一个RequestId，为了防止重复请求

	ApplyLogIndex int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, b := kv.rf.GetState(); !b {
		//说明不是Leader
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("ShardKV[%d]接收到了cilent[%d]的get操作:%v", kv.me, args.ClientId, args)
	//先检查这个请求是否已经执行过了
	kv.mu.Lock()
	if kv.RepeatCheckL(args.ClientId, args.RequestId) {
		//true 直接返回数据库中的结果即可
		reply.Err = OK
		reply.Value = kv.KVDB[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//把日志写到raft服务器中
	Log := Op{
		Operation: "Get",
		Key: args.Key,
		Value: "",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	logIndex, _, _ := kv.rf.Start(Log)
	kv.mu.Lock()
	ChLogIndex, exist := kv.waitApplyCh[logIndex]
	if !exist {
		DPrintf("kv[%d].waitApplyCh[%d]不存在", kv.me, logIndex)
		kv.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = kv.waitApplyCh[logIndex]
	}
	kv.mu.Unlock()
	//只有应用到raft状态机上才能保存到数据库中并且返回给client结果,所以等待ChLogIndex上传入的信息
	select {
	case <- time.After(time.Millisecond*600):
		DPrintf("kvserver[%d]接收到了cilent[%d]的get操作:%v超时返回timeout", kv.me, args.ClientId, args)
		reply.Err = ErrWrongLeader
	case op := <- ChLogIndex:
		if _, b :=kv.rf.GetState(); !b {
			reply.Err = ErrWrongLeader
		}else {
			if op.ClientId == Log.ClientId && op.RequestId == Log.RequestId {
				//这样才能唯一定位一个操作是否已经执行了，防止旧leader同步日志的时候把新leader上logIndex位置上的日志当作这个日志
				kv.mu.Lock()
				if value, exist := kv.KVDB[args.Key]; exist {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
				}
				kv.mu.Unlock()
			} else {
				reply.Err = ErrWrongLeader
			}
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, logIndex)
	kv.mu.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.			servers[] 包含该组中服务器的端口。
//
// me is the index of the current server in servers[].					me 是 servers[] 中当前服务器的索引。
//
// the k/v server should store snapshots through the underlying Raft	k/v 服务器应该通过底层的 Raft 实现来存储快照，
// implementation, which should call persister.SaveStateAndSnapshot() to	它应该调用 persister.SaveStateAndSnapshot()
// atomically save the Raft state along with the snapshot.				来原子地保存 Raft 状态和快照。
//
// the k/v server should snapshot when Raft's saved state exceeds		当 Raft 的保存状态超过 maxraftstate 字节时，
// maxraftstate bytes, in order to allow Raft to garbage-collect its	k/v 服务器应该进行快照，以允许 Raft 对其日志进行垃圾收集。
// log. if maxraftstate is -1, you don't need to snapshot.				如果 maxraftstate 为 -1，则不需要快照。
//
// gid is this group's GID, for interacting with the shardmaster.		gid 是该组的 GID，用于与 shardmaster 交互。
//
// pass masters[] to shardmaster.MakeClerk() so you can send			将 masters[] 传递给 shardmaster.MakeClerk()
// RPCs to the shardmaster.												以便您可以将 RPC 发送到 shardmaster。
//
// make_end(servername) turns a server name from a						make_end(servername) 将服务器名称从
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can		Config.Groups[gid][i] 转换为您可以发送 RPC 的
// send RPCs. You'll need this to send RPCs to other groups.			labrpc.ClientEnd。 您将需要它来将 RPC 发送到其他组。
//
// look at client.go for examples of how to use masters[]				查看 client.go 以获取有关如何使用 masters[]
// and make_end() to send RPCs to the group owning a specific shard.	和 make_end() 将 RPC 发送到拥有特定分片的组的示例。
//
// StartServer() must return quickly, so it should start goroutines		StartServer() 必须快速返回，
// for any long-running work.											因此它应该为任何长时间运行的工作启动 goroutine。
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:		使用类似这样的方式与 shardmaster 对话：
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	return kv
}
