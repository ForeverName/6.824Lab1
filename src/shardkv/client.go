package shardkv

//
// client code to talk to a sharded key/value service.		与分片键/值服务对话的客户端代码。
//
// the client first talks to the shardmaster to find out	客户端首先与 shardmaster 对话以找出分片（keys）到组的分配，
// the assignment of shards (keys) to groups, and then		然后与持有key分片的组对话。
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

//
// which shard is a key in?		key在哪个分片中？ 请使用此功能，请勿更改。
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.		测试人员调用 MakeClerk。
//
// masters[] is needed to call shardmaster.MakeClerk().		masters[] 需要调用 shardmaster.MakeClerk()。
//
// make_end(servername) turns a server name from a			make_end(servername) 将服务器名称从 Config.Groups[gid][i]
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can	转换为您可以发送 RPC 的 labrpc.ClientEnd。
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.						获取key的当前值。
// returns "" if the key does not exist.					如果键不存在，则返回 ""。
// keeps trying forever in the face of all other errors.	面对所有其他错误，不断尝试。
// You will have to modify this function.					您将不得不修改此功能。
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.   尝试每个服务器的分片。
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.  向master请求最新配置。
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.				由 Put 和 Append 共享。
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op


	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
