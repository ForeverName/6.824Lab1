package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	requestId int //为了处理重复命令，对每个命令有一个唯一的序号
	recentLeaderId int //记录最近一次的领导者id
	clientId int64 //唯一定义每个client的id值
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.								获取键的当前值。
// returns "" if the key does not exist.							如果键不存在，则返回 ""。
// keeps trying forever in the face of all other errors.			面对所有其他错误，不断尝试。
//
// you can send an RPC with code like this:							您可以使用如下代码发送 RPC：
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//args 和 reply 的类型（包括它们是否是指针）必须与 RPC 处理函数参数的声明类型匹配。 并且回复必须作为指针传递。
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	//RPC调用服务端
	ck.requestId++
	args := GetArgs{
		Key: key,
		RequestId: ck.requestId,
		ClientId: ck.clientId,
	}
	DPrintf("client[%d]执行get操作,传入参数为:%v", ck.clientId, args)
	for true {
		reply := GetReply{}
		ok := ck.servers[ck.recentLeaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			ck.recentLeaderId = (ck.recentLeaderId + 1) % len(ck.servers)
		} else if reply.Err == ErrNoKey {
			return ""
		} else if reply.Err == OK{
			DPrintf("client[%d]执行get[%s]结果已经返回Value为:%s", ck.clientId, args.Key, reply.Value)
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestId++
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	DPrintf("client[%d]向kvserver[%d]执行%s操作,RPC传入参数为%v", ck.clientId, ck.recentLeaderId, op, args)
	for true {
		reply := PutAppendReply{}
		ok := ck.servers[ck.recentLeaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.recentLeaderId = (ck.recentLeaderId + 1) % len(ck.servers)
		} else if reply.Err == OK {
			DPrintf("client[%d]向kvserver[%d]执行%v操作的结果已经返回",ck.clientId,ck.recentLeaderId,args)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
