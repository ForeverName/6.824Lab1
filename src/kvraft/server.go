package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//这里定义的是存储到日志中的内容
	Operation string //put get append
	Key string
	Value string
	ClientId int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big  如果日志增长这么大，快照

	// Your definitions here.
	KVDB map[string]string //模拟一个数据库，数据都存在map里面
	waitApplyCh map[int]chan Op // map[logIndex]chan 对于每一个logIndex建立一个对应的通道来通知已经完成
	DuplicateDetection map[int64]int //存储每一个clientId对应的最后一个RequestId，为了防止重复请求
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, b := kv.rf.GetState(); !b {
		//说明不是Leader
		reply.Err = ErrWrongLeader
	}
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
		kv.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = kv.waitApplyCh[logIndex]
	}
	kv.mu.Unlock()
	//只有应用到raft状态机上才能保存到数据库中并且返回给client结果,所以等待ChLogIndex上传入的信息
	select {
	case  <- ChLogIndex:
		if _, b :=kv.rf.GetState(); !b {
			reply.Err = ErrWrongLeader
		}
		reply.Err = OK
		reply.Value = kv.KVDB[args.Key]
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, b := kv.rf.GetState(); !b {
		//说明不是Leader
		reply.Err = ErrWrongLeader
	}
	Log := Op{
		Operation: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	logIndex, _, _ := kv.rf.Start(Log)
	kv.mu.Lock()
	ChLogIndex, exist := kv.waitApplyCh[logIndex]
	if !exist {
		kv.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = kv.waitApplyCh[logIndex]
	}
	kv.mu.Unlock()
	select {
	case  <- ChLogIndex:
		if _, b :=kv.rf.GetState(); !b {
			reply.Err = ErrWrongLeader
		}
		reply.Err = OK
		if args.Op == "Put" {
			kv.KVDB[args.Key] = args.Value
		} else {
			kv.KVDB[args.Key] += args.Value
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of						servers[] 包含一组服务器的端口，
// servers that will cooperate via Raft to							这些服务器将通过 Raft 协作以形成容错键/值服务。
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].						me 是 servers[] 中当前服务器的索引。
// the k/v server should store snapshots through the underlying Raft		k/v 服务器应该通过底层的 Raft 实现来存储快照，
// implementation, which should call persister.SaveStateAndSnapshot() to	它应该调用 persister.SaveStateAndSnapshot()
// atomically save the Raft state along with the snapshot.					来原子地保存 Raft 状态和快照。
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,		当 Raft 的保存状态超过 maxraftstate 字节时，k/v 服务器应该进行快照，
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,				以允许 Raft 对其日志进行垃圾收集。 如果 maxraftstate 为 -1，则不需要快照。
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines		StartKVServer() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KVDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.DuplicateDetection = make(map[int64]int)
	go kv.ConsumeApply()
	return kv
}
//处理apply返回的信息
func (kv *KVServer) ConsumeApply() {
	select {
	case msg := <-kv.applyCh:
		logIndex := msg.CommandIndex
		op := msg.Command.(Op)
		kv.waitApplyCh[logIndex] <- op
	}
}