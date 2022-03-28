package kvraft

import "fmt"

func (p PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs{Key:%s, Value:%s, Op:%s, ClientId:%d, RequestId:%d}",
		p.Key, p.Value, p.Op, p.ClientId, p.RequestId)
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

func (o Op) String() string {
	return fmt.Sprintf("Op{Operation:%s, Key:%s, Value:%s, ClientId:%d, RequestId:%d}",
		o.Operation, o.Key,o.Value, o.ClientId, o.RequestId)
}