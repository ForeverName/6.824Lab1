package shardmaster

//
// Master shard server: assigns shards to replication groups.				主分片服务器：将分片分配给复制组。
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).	add a set of groups（gid -> 服务器列表映射）。
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.	将当前所有者的一个碎片交给 gid。
// Query(num) -> fetch Config # num, or latest config if num==-1.		获取 Config # num，如果 num==-1，则获取最新配置。
//
// A Config (configuration) describes a set of replica groups, and the 一个 Config（配置）描述了一组副本组，以及负责每个分片的副本组。
// replica group responsible for each shard. Configs are numbered. Config 配置已编号。
// #0 is the initial configuration, with no groups and all shards		Config #0 是初始配置，没有组，所有分片都分配给组 0（无效组）。
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.				您需要将字段添加到 RPC 参数结构。
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.			配置——将分片分配给组。
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	RequestId int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	RequestId int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	RequestId int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientId int64
	RequestId int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
