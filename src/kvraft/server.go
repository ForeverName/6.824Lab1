package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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

	// Lab3B
	ApplyLogIndex int
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
		return
	}
	DPrintf("kvserver[%d]接收到了cilent[%d]的get操作:%v", kv.me, args.ClientId, args)
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	DPrintf("kvserver[%d]接收到client[%d]的%s操作:%v", kv.me, args.ClientId, args.Op, args)
	//判断是否是已经执行过的请求
	kv.mu.Lock()
	if kv.RepeatCheckL(args.ClientId, args.RequestId) {
		//true 直接返回数据库中的结果即可
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
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
		DPrintf("kv[%d].waitApplyCh[%d]不存在", kv.me, logIndex)
		kv.waitApplyCh[logIndex] = make(chan Op, 1)
		ChLogIndex = kv.waitApplyCh[logIndex]
	}
	kv.mu.Unlock()
	select {
	case <- time.After(time.Millisecond*600):
		//如果超过这个时间应该再次检查Leader是否已经改变,
		//比如logIndex=194发送到leader之后leader重新选举了，导致这个日志一直没有commit，
		//如果client不重新发送的话，选举好之后新的leader就不会接收到这个日志，也就不会应用，
		//此时apply会一直阻塞住等待leader应用新的日志（但没有所以会一直阻塞），<- ChLogIndex也会一直阻塞，
		//最终结果就是状态机一直在无用的工作
		DPrintf("kvserver[%d]接收到了client[%d]的%s操作:%v超时返回timeout", kv.me, args.ClientId, args.Op, args)
		reply.Err = ErrWrongLeader
	case op:= <- ChLogIndex:
		if _, b :=kv.rf.GetState(); !b {
			reply.Err = ErrWrongLeader
		}else {
			if op.ClientId == Log.ClientId && op.RequestId == Log.RequestId {
				//如果不做这个判断，会导致以下情形出现错误：
				//当旧leader不断接收到一个只能连接到这个服务器的client[1]的操作时put{key:1,value:15}，由于还没到超时时间，
				//旧leader和新leader连接上了，这时新leader会同步日志给旧leader，而这个旧的put{key:1,value:15}目前的logIndex为5，
				//此时新的leader已经有了logindex为5的日志，这时如果<- ChLogIndex接收到内容(旧的leader也有waitApplyCh[5]在等待内容)，
				//会误以为put{key:1,value:15}应用成功，而实际上并没有，所以需要判断上面两个条件，上面两个条件唯一判定一个客户端的一个操作
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
		}
	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, logIndex)
	kv.mu.Unlock()
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
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.InstallSnapshot(snapshot, kv.rf.LastIncludedIndex)
	}
	go kv.ConsumeApply()
	return kv
}

//处理apply返回的信息
func (kv *KVServer) ConsumeApply() {
	for msg := range kv.applyCh {
		DPrintf("%v已经应用到kvserver[%d]", msg, kv.me)
		if !msg.CommandValid {
			// 安装快照
			kv.InstallSnapshot(msg.Snapshot, msg.LastIncludedIndex)
			continue
		}
		logIndex := msg.CommandIndex
		op := msg.Command.(Op)
		clientId := op.ClientId
		requestId := op.RequestId
		if kv.maxraftstate != -1 && kv.rf.CheckLogSize(kv.maxraftstate) {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.KVDB)
			e.Encode(kv.DuplicateDetection)
			snapshot := w.Bytes()
			applyLogIndex := kv.ApplyLogIndex
			kv.mu.Unlock()
			go func(snapshot []byte, applyLogIndex int) {
				kv.rf.TakeSnapshotAndTruncateTheLog(snapshot, applyLogIndex)
			}(snapshot, applyLogIndex)
		}
		kv.mu.Lock()
		kv.ApplyLogIndex = logIndex
		if kv.RepeatCheckL(clientId, requestId) {
			kv.mu.Unlock()
			continue
		} else {
			//执行对应command的操作
			switch op.Operation {
			case "Put":
				kv.KVDB[op.Key] = op.Value
			case "Append":
				kv.KVDB[op.Key] += op.Value
			}
			kv.DuplicateDetection[clientId] = requestId
			DPrintf("kvserver[%d]的map为:%v", kv.me, kv.KVDB)
		}
		if _, exit := kv.waitApplyCh[logIndex]; !exit {
			//针对不是Leader的服务端，其kv.waitApplyCh也没有需要等待通知返回给客户端的chan
			kv.mu.Unlock()
			continue
		}
		ops := kv.waitApplyCh[logIndex]
		ops <- op
		kv.mu.Unlock()
	}
}