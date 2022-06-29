package shardmaster

import "fmt"

func (j JoinArgs) String() string {
	return fmt.Sprintf("JoinArgs{Servers:%v, ClientId:%d, RequestId:%d}", j.Servers, j.ClientId, j.RequestId)
}

func (j JoinReply) String() string {
	return fmt.Sprintf("JoinReply{WrongLeader:%t, Err:%s}", j.WrongLeader, j.Err)
}

func (l LeaveArgs) String() string {
	return fmt.Sprintf("LeaveArgs{GIDs:%v, ClientId:%d, RequestId:%d}", l.GIDs, l.ClientId, l.RequestId)
}

func (l LeaveReply) String() string {
	return fmt.Sprintf("LeaveReply{WrongLeader:%t, Err:%s}", l.WrongLeader, l.Err)
}

func (m MoveArgs) String() string {
	return fmt.Sprintf("MoveArgs{Shard:%d, GID:%d, ClientId:%d, RequestId:%d}", m.Shard, m.GID, m.ClientId, m.RequestId)
}

func (m MoveReply) String() string {
	return fmt.Sprintf("MoveReply{WrongLeader:%t, Err:%s}", m.WrongLeader, m.Err)
}

func (q QueryArgs) String() string {
	return fmt.Sprintf("QueryArgs{Num:%d, ClientId:%d, RequestId:%d}", q.Num, q.ClientId, q.RequestId)
}

func (q QueryReply) String() string {
	return fmt.Sprintf("QueryReply{WrongLeader:%t, Err:%s, Config:%v}", q.WrongLeader, q.Err, q.Config)
}

func (c Config) String() string {
	return fmt.Sprintf("Config{Num:%d, Shards:%v, Groups:%v}", c.Num, c.Shards, c.Groups)
}