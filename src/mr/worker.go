package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
//自己写的
type worker struct {
	workerID int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	//fmt.Println("worker开始请求任务......")
	work := worker{mapf: mapf, reducef: reducef}
	/**
	向master请求任务(通过RPC调用完成）
	针对返回来的任务类型，做不同的处理
	返回任务类型是map：调用Map函数
	返回任务类型是reduce：调用Reduce函数
	还有两种特殊情况，就是任务等待和任务终止，分别延时一段时间和直接返回
	默认情况抛异常
	 */
	for true {
		task := CallTaskForMaster()
		//用于处理多线程下的一个bug，bug如下：    猜想原因是由于一个线程的reply为exitTask正常退出，而其他线程的reply为空，导致打不开文件名为 空 的文件。
		//出错文件信息是: {0 0 0  0 0 0 0001-01-01 00:00:00 +0000 UTC}
		//2022/03/03 16:15:40 cannot open
		if task.Filename == "" && task.TaskType == 0 {
			break
		}

		if task.TaskType == 0 {
			//map任务
			//fmt.Println("开始执行map任务")
			work.mapTask(&task)
			//fmt.Println("map任务执行完成")
			//处理完map任务，通知master已经完成
			CallMapTaskDone(&task)

		}else if task.TaskType == 1 {
			//处理reduce任务
			//fmt.Println("开始执行reduce任务")
			work.reduceTask(&task)
			//fmt.Println("reduce任务执行完成")
			//处理完也要通知master
			CallReduceTaskDone(&task)
		}else if task.TaskType == 2 {
			//等待1s继续请求任务
			//fmt.Println("等待1s继续请求任务")
			time.Sleep(time.Second)
		}else if task.TaskType == 3 {
			//退出
			break
		}
	}
}

func CallReduceTaskDone(t *Task) {
	args := t
	var reply Task
	call("Master.ReduceTaskDone", args, &reply)
}

//处理map任务
func (w *worker) mapTask(task *Task) {
	//fmt.Println("mapTask函数开始执行...")
	intermediate := []KeyValue{}
	file, err := os.Open(task.Filename)
	if err != nil {
		fmt.Println("出错文件信息:", *task)
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := w.mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := task.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outindex := 0; outindex < nReduce; outindex++ {
		//创建临时文件并绑定对应的JSON编码器
		outFiles[outindex], err = ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Fatal("Cannot create temporary file ", err)
		}
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	//写入临时文件
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", task.Filename, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	//写入正式文件
	for outindex, file := range outFiles {
		//task.ReduceIndex = outindex
		outname := "mr-"+strconv.Itoa(task.MapIndex)+"-" + strconv.Itoa(outindex)
		//outname := mr-0-0
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}

	//fmt.Println("mapTask函数执行完成...")
}

//处理reduce任务
func (w *worker) reduceTask(t *Task) {
	//innameprefix := "mr-tmp/mr-"
	innameprefix := "mr-"
	innamesuffix := "-" + strconv.Itoa(t.ReduceIndex)
	reduceIndex := t.ReduceIndex
	outname := "mr-out-" + strconv.Itoa(reduceIndex)
	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < t.NumberFiles; index++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		//inname = mr-0-0
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile(".", "mr-*")
	if err != nil {
		//fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
}

//通过RPC向master请求任务
func CallTaskForMaster() Task{
	//fmt.Println("worker请求任务远程调用开始......")
	args := ExampleArgs{}
	reply := Task{}
	call("Master.TaskForMaster", &args, &reply)
	//fmt.Println("远程调用返回的信息为：", reply)
	return reply
}

//通知master任务已经完成
func CallMapTaskDone(task *Task) {
	args := task
	reply := Task{}
	call("Master.MapTaskDone", &args, &reply)
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
