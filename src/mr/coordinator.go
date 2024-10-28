package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mu sync.Mutex

const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Task struct {
	TaskType      int
	InputFileName string // 输入文件
	MapTaskId     int
	NReduce       int
	NMap          int
	StartTime     time.Time
}

type CoordinatorSchedule int

const (
	MapSchedule = iota
	ReduceSchedule
	WaitSchedule
	Completed
)

type Coordinator struct {
	lock       sync.Mutex
	State      CoordinatorSchedule
	InputFiles []string // Map 阶段存储输入文件，Reduce 阶段存储 reduceId
	FileIdx    map[string]int
	TaskQueue  map[string]Task // 存放的是未完成的 task，为了检测超时任务
	NReduce    int
	NMap       int // 有多少个 input 文件对应有几个 map task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, task *Task) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch c.State {
	case MapSchedule:
		if len(c.InputFiles) == 0 {
			task.TaskType = WaitTask
			break
		}

		filename := c.InputFiles[0]

		// task 信息
		*task = Task{
			TaskType:      MapTask,
			InputFileName: filename,
			StartTime:     time.Now(),
			MapTaskId:     c.FileIdx[filename],
			NMap:          c.NMap,
			NReduce:       c.NReduce,
		}
		// fmt.Printf("filename = %v\n", filename)
		c.TaskQueue[filename] = *task

		// 移除已经读入的文件，防止并发重复读
		c.InputFiles = c.InputFiles[1:]

	case ReduceSchedule:
		if len(c.InputFiles) == 0 {
			task.TaskType = WaitTask
			break
		}

		*task = Task{
			TaskType:      ReduceTask,
			InputFileName: c.InputFiles[0],
			StartTime:     time.Now(),
			NReduce:       c.NReduce,
			NMap:          c.NMap,
		}

		c.TaskQueue[c.InputFiles[0]] = *task
		c.InputFiles = c.InputFiles[1:]
	case WaitSchedule:
		task.TaskType = WaitTask
	case Completed:
		task.TaskType = ExitTask
	}
	// log.Printf("now state = %v, send task %v\n", c.State, task.TaskType)

	return nil
}

func (c *Coordinator) GetReportTask(task *Task, reply *Task) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch c.State {
	case MapSchedule:
		// log.Printf("delete task %v\n", task.InputFileName)
		delete(c.TaskQueue, task.InputFileName)
		if c.State == MapSchedule && len(c.TaskQueue) == 0 && len(c.InputFiles) == 0 {
			c.State = ReduceSchedule
			for i := 0; i < c.NReduce; i++ {
				c.InputFiles = append(c.InputFiles, strconv.Itoa(i))
			}
		}
	case ReduceSchedule:
		delete(c.TaskQueue, task.InputFileName)
		if c.State == ReduceSchedule && len(c.TaskQueue) == 0 && len(c.InputFiles) == 0 {
			// log.Printf("all reduce task done, finish all")
			c.State = Completed
		}
	case Completed:
		log.Println("finish all")
		// c.Done()
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
	// 启动心跳检测
	go c.heartbeat()

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.State == Completed
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var InputFiles []string
	var NMap int
	var FileIdx = make(map[string]int)
	// Your code here.
	for idx, filename := range files {
		FileIdx[filename] = idx
		InputFiles = append(InputFiles, filename)
		NMap++
	}
	c := Coordinator{
		InputFiles: InputFiles,
		TaskQueue:  make(map[string]Task),
		FileIdx:    FileIdx,
		NReduce:    nReduce,
		NMap:       NMap,
	}
	c.server()
	return &c
}

func (c *Coordinator) heartbeat() {
	// 超时任务检测
	for {
		c.lock.Lock()
		for filename, task := range c.TaskQueue {
			if time.Since(task.StartTime) > 10*time.Second {
				// log.Printf("task %v timeout\n", filename)
				delete(c.TaskQueue, filename)
				c.InputFiles = append(c.InputFiles, filename)
			}
		}
		c.lock.Unlock()
		time.Sleep(3 * time.Second)
	}
}
