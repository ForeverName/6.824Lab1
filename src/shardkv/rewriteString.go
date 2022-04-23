package shardkv

import "fmt"

func (p PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs{ClientId:%d, RequestId:%d, Key:%s, Value:%s, Op:%s}",
		p.ClientId, p.RequestId, p.Key, p.Value, p.Op)
}

func (p PutAppendReply) String() string {
	return fmt.Sprintf("PutAppendReply{Err:%s}", p.Err)
}

func (g GetArgs) String() string {
	return fmt.Sprintf("GetArgs{Key:%s, RequestId:%d, ClientId:%d}", g.Key, g.RequestId, g.ClientId)
}

func (g GetReply) String() string {
	return fmt.Sprintf("GetReply{Err:%s, Value:%s}", g.Err, g.Value)
}