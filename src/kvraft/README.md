目前存在的bug：
1.在几千次测试中会出现TestManyPartitionsManyClients3A这个测试一直卡着不动，CPU占用率拉满，
原因查询网上说的是：raft选举后假如当前term没有新start的entry，那么之前term遗留下的entry永远不会commit。
这样会导致之前的请求一直等待，无法返回。 所以每次raft选举后，向ApplyCh发送一个消息，提醒server主动start一个新的entry，
并且这个entry里面应该有当前server的id，即kv. me。
目前还未测试这个方法的正确性，等有时间再来完善。