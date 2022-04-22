package shardmaster

import (
	"log"
	"math"
	"sort"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//检查请求是否已经执行了
func (sm *ShardMaster) RepeatCheckL(clientId int64, requestId int) bool {
	lastRequestId, exist := sm.DuplicateDetection[clientId]
	if !exist {
		return false
	}
	if lastRequestId >= requestId {
		//说明这个客户端的请求已经完成了
		return true
	}
	return false
}

// 重新平衡 Config里面的Groups
func (sm *ShardMaster) ReBalance(config *Config, gid int,op string) {
	shardsCount := sm.groupByGid(config)
	switch op {
	case "Join":
		avg := NShards / len(config.Groups)
		for i := 0; i < avg; i++ {
			maxGid := sm.getMaxShardGid(shardsCount)
			config.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case "Leave":
		shardsArray,exists := shardsCount[gid]
		if !exists {return}
		delete(shardsCount,gid)
		// 说明已经没有Group了，初始化为原来一开始全部是0的时候
		if len(config.Groups) == 0 {
			config.Shards = [NShards]int{}
			return
		}
		for _,v := range shardsArray {
			minGid := sm.getMinShardGid(shardsCount)
			config.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid], v)
		}
	}
}

// 找出拥有最多分片的一个群组gid
func (sm *ShardMaster) getMaxShardGid(shardsCount map[int][]int) int {
	// 要排序，防止两个不同节点选择的gid不一样导致config不同
	gids := make([]int, 0)
	for k, _ := range shardsCount {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	max := -1
	var gid int
	for _, g := range gids {
		if max < len(shardsCount[g]) {
			max = len(shardsCount[g])
			gid = g
		}
	}
	return gid
}

// 统计出来每个gid对应的分片有哪些 如:map[gid][]int{0,5,6}
func (sm *ShardMaster) groupByGid(cfg *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k,_ := range cfg.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}

// 复制一个新的Config和最新的一致
func (sm *ShardMaster) createNextConfig() Config {
	lastCfg := sm.configs[len(sm.configs)-1]
	nextCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}

// 找出拥有最少分片的一个群组gid
func (sm *ShardMaster) getMinShardGid(shardsCount map[int][]int) int {
	// 要排序，防止两个不同节点选择的gid不一样导致config不同
	gids := make([]int, 0)
	for k, _ := range shardsCount {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	min := math.MaxInt32
	var gid int
	for _, g := range gids {
		if min > len(shardsCount[g]) {
			min = len(shardsCount[g])
			gid = g
		}
	}
	return gid
}

func (sm *ShardMaster) JoinOP(servers map[int][]string) {
	nextConfig := sm.createNextConfig()
	for gid, servers := range servers {
		// 实现加入操作，并且重新平衡
		nextConfig.Groups[gid] = append(nextConfig.Groups[gid], servers...)
		sm.ReBalance(&nextConfig, gid, "Join")
	}
	sm.configs = append(sm.configs,nextConfig)
}

func (sm *ShardMaster) LeaveOP(gids []int) {
	// Leave实际逻辑实现
	nextConfig := sm.createNextConfig()
	for _, gid := range gids {
		delete(nextConfig.Groups, gid)
		sm.ReBalance(&nextConfig, gid, "Leave")
	}
	sm.configs = append(sm.configs,nextConfig)
}

func (sm *ShardMaster) MoveOP(gid int, shardId int) {
	// Move实际逻辑实现
	nextConfig := sm.createNextConfig()
	if _,exists := nextConfig.Groups[gid]; exists {
		nextConfig.Shards[shardId] = gid
	}
	sm.configs = append(sm.configs,nextConfig)
}