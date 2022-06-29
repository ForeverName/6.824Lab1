package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.		许多副本组，每个都运行一次操作的 paxos。
// Shardmaster decides which group serves each shard.			Shardmaster 决定哪个组为每个分片服务。
// Shardmaster may change shard assignment from time to time.	Shardmaster 可能会不时更改分片分配。
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
