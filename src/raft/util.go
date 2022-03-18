package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//获取随机睡眠时间
func (rf *Raft) GetRandSleepTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(50)+ 20) * time.Millisecond
}

//获得随机选举超时时间
func (rf *Raft) GetRandElection() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(200+rand.Int31n(150)) * time.Millisecond
}

//初始化nextIndex数组
func (rf *Raft) InitNextIndexL() {
	for i, _ := range rf.nextIndex {
		if len(rf.log) == 0 {
			rf.nextIndex[i] = 1
		}else {
			rf.nextIndex[i] = rf.log[len(rf.log) - 1].LogIndex + 1
		}
	}
}