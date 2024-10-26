package mr

import (
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Task struct {
	TaskType          int
	InputFileName     string // 输入文件
	NReduce           int
	NMap              int
	MapTaskId         int
	IntermediateFiles []string
}

type CoordinatorSchedule int

const (
	MapSchedule = iota
	ReduceSchedule
	WaitSchedule
	Completed
)

type Coordinator struct {
	State        CoordinatorSchedule
	InputFiles   []string
	FilesMapping map[string]int
	MapTaskId    int // 说明现在进行到第几个 map task 了
	NReduce      int
	NMap         int // 有多少个 input 文件对应有几个 map task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.State {
	case MapSchedule:
		if c.MapTaskId < c.NMap {
			task.TaskType = MapTask
			task.InputFileName = c.InputFiles[c.MapTaskId]
			task.MapTaskId = c.MapTaskId
			c.MapTaskId++
		}
	case ReduceSchedule:
		task.TaskType = ReduceTask
	case WaitSchedule:
		task.TaskType = WaitTask
	case Completed:
		task.TaskType = ExitTask
	}
	// log.Printf("now state = %v, send task %v\n", c.State, task.TaskType)
	task.NReduce = c.NReduce
	task.NMap = c.NMap
	return nil
}

func (c *Coordinator) GetReportTask(args *TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	//log.Printf("now state = %v, get report task %v", c.State, task.TaskType)
	switch c.State {
	case MapSchedule:
		if c.MapTaskId >= c.NMap {
			c.State = ReduceSchedule
		}
	case ReduceSchedule:
		c.State = WaitSchedule
	case WaitSchedule:
		time.Sleep(3 * time.Second)
		c.State = Completed
	case Completed:
		log.Println("finish all")
		c.Done()
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.State == Completed {
		ret = true
		time.Sleep(10 * time.Second)
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.FilesMapping = make(map[string]int)
	c.NReduce = nReduce
	c.State = MapSchedule

	// Your code here.
	for _, filePattern := range files {
		inputFiles, err := filepath.Glob(filePattern)
		if err != nil {
			log.Fatal("cannot find input file,whose name like %v", filePattern)
		}
		for _, filename := range inputFiles {
			c.InputFiles = append(c.InputFiles, filename)
			c.FilesMapping[filename] = c.NMap
			c.NMap++
		}
	}

	c.server()
	return &c
}
