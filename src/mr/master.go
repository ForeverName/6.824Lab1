package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	NumWorkers int // 用于给worker进行编号
	MapTasks    []Task // 存放map任务
	ReduceTasks []Task // 存放reduce任务
	Mutex sync.Mutex // 用于保证有些操作之间互斥
	NReduce int // Reduce任务的数量
	//为了提示快速分配对应类型的任务，为0表示还存在map任务没有分配，为1表示所有map任务都已经分配完成，但不确定map任务是否都已经执行完成，
	//需要遍历一遍来判断，如果所有任务都已经完成，则把taskPhase置为2
	//为2表示map任务全部执行完毕，需要分配reduce任务了
	//为3表示所有reduce全都分配完毕，但不知道是否全部执行完成，需要遍历一遍来判断，如果全部完成，则把taskPhase置为4
	//为4表示一切都已经完成，通知master退出程序
	TaskPhase int
	NumTask int //记录任务数量
}

// Your code here -- RPC handlers for the worker to call.
//这个方法是分配任务给worker，若还存在map任务，则先分配map任务给worker，只有map任务全部完成之后才分配reduce任务给worker
func (m *Master) TaskForMaster(args *ExampleArgs, reply *Task) error {
	//fmt.Println("master分配任务开始")
	//fmt.Println("taskPhase值为：", m.TaskPhase)
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	task := Task{
		TaskType: 2,
	}
	exitTask := Task{
		TaskType: 3,
	}
	//先查看taskPhase
	if m.TaskPhase == 0 {
		//说明还有没有分配的map任务
		for i, mapTask := range m.MapTasks {
			//fmt.Println("mapTask.TaskState的值为:", mapTask.TaskState)
			//fmt.Println("mapTasks数组的信息:", m.MapTasks)
			//go的range是拷贝而非引用,所以这里修改之后原来的MapTasks并没有改变。要使用原来切片下标来修改
			if mapTask.TaskState == 0 {
				//像这样
				m.MapTasks[i].NReduce = m.NReduce
				m.MapTasks[i].TaskState = 1
				m.MapTasks[i].MapIndex = i
				m.MapTasks[i].BeginTime = time.Now()
				//而不是这样
				/*mapTask.NReduce = m.NReduce
				mapTask.TaskState = 1
				mapTask.MapIndex = i*/
				*reply = m.MapTasks[i]
				//fmt.Println("返回reply", *reply)
				return nil
			}
		}
		*reply = task
		m.TaskPhase = 1
		return nil
	}else if m.TaskPhase == 1 {
		//遍历一遍mapTasks判断map任务是否都已经执行完成
		//说明还有正在执行的map任务，返回一个特殊的任务，让worker sleep 1s后再向master请求任务
		*reply = task
		for _, mapTask := range m.MapTasks {
			if mapTask.TaskState == 1{
				return nil
			}
		}
		m.TaskPhase = 2
		return nil
	}else if m.TaskPhase == 2 {
		//分配reduce任务
		//fmt.Println("reduce任务分配开始")
		for i, reduceTask := range m.ReduceTasks {
			if reduceTask.TaskState == 0 {
				//fmt.Println("分配的reduce任务信息为:", reduceTask)
				m.ReduceTasks[i].TaskState = 1
				m.ReduceTasks[i].BeginTime = time.Now()
				*reply = m.ReduceTasks[i]
				return nil
			}
		}
		m.TaskPhase = 3
		return nil
	}else if m.TaskPhase == 3 {
		for _, reduceTask := range m.ReduceTasks {
			if reduceTask.TaskState == 1 {
				*reply = task
				return nil
			}
		}
		*reply = exitTask
		m.TaskPhase = 4
		return nil
	}else {
		//taskPhase == 4
		*reply = exitTask
		return nil

	}
	//fmt.Println("master分配任务完成")
	return nil
}

//周期性检测worker是否超时
func (m *Master) cashCheck()  {
	for true {
		m.Mutex.Lock()
		//只有TaskState=1即在运行中的任务才会被检测
		for i, task := range m.MapTasks {
			if task.TaskState == 1 && time.Now().Sub(task.BeginTime) > 10*time.Second {
				//如果超时，则把这个任务的状态重新置为0
				m.MapTasks[i].TaskState = 0
				m.TaskPhase = 0
			}
		}

		for i, task := range m.ReduceTasks {
			if task.TaskState == 1 && time.Now().Sub(task.BeginTime) > 10*time.Second {
				//如果超时，则把这个任务的状态重新置为0
				m.ReduceTasks[i].TaskState = 0
				m.TaskPhase = 2
			}
		}
		m.Mutex.Unlock()
		time.Sleep(2*time.Second)
	}
}
//worker向master汇报任务已经完成
func (m *Master) MapTaskDone(task *Task, reply *Task) error {
	//如果是超时返回的那么就直接舍弃
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if time.Now().Sub(task.BeginTime) > 10*time.Second {
		return nil
	}
	mapIndex := task.MapIndex
	m.MapTasks[mapIndex].TaskState = 2
	return nil
}

//worker向master汇报任务已经完成
func (m *Master) ReduceTaskDone(task *Task, reply *Task) error {
	//如果是超时返回的那么就直接舍弃
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if time.Now().Sub(task.BeginTime) > 10*time.Second {
		return nil
	}
	reduceIndex := task.ReduceIndex
	m.ReduceTasks[reduceIndex].TaskState = 2

	//检查是否所有reduce任务都已经完成，如果完成则退出主程序
	for _, reduceTask := range m.ReduceTasks {
		if reduceTask.TaskState == 1 || reduceTask.TaskState == 0 {
			return nil
		}
	}
	m.TaskPhase = 4
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	ret := false
	// Your code here.
	if m.TaskPhase == 4 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	//files中存储的就是许多的文件名字
	//这里要初始化信息，包括从files中记录任务信息，一个文件对应一个map任务
	//还要记录这个任务是什么类型的任务，是否已经完成等等
	//fmt.Println("master初始化开始......")
	m.NReduce = nReduce
	m.NumTask = len(files)
	for _, fileName := range files{
		//fmt.Println("Filename:", fileName)
		mapTask := Task{TaskType: 0, Filename: fileName, TaskState: 0}
		m.MapTasks = append(m.MapTasks, mapTask)
	}
	//初始化
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{
			ReduceIndex: i,
			TaskType: 1,
			TaskState: 0,
			Filename: "mr-*-" + strconv.Itoa(i),
			NumberFiles: len(files),
		}
		m.ReduceTasks = append(m.ReduceTasks, reduceTask)
	}
	//fmt.Println("master初始化完成......")
	m.server()
	go m.cashCheck()
	return &m
}
