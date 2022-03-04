package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//自己写的:用于存放任务信息
type Task struct {
	MapIndex int //记录map任务的下标
	ReduceIndex int //记录reduce任务的下标
	TaskType int // MAPTASK-0 REDUCETASK-1 NONETASK-2 EXITTASK-3
	Filename string // one filename for one maptask
	//FailedWorkers []int // record workerID who failed to process this task in 10 seconds
	TaskState int // Idle-0 Running-1 Finished-2
	NReduce int
	NumberFiles int
	BeginTime time.Time //记录这个任务开始分配给worker的时间，用来验证是否超时
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
