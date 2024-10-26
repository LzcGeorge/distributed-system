package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := getTaskFromCoordinator()
		// fmt.Printf("receive task: %v\n", task.TaskType)
		switch task.TaskType {
		case MapTask:
			mapper(&task, mapf)
		case ReduceTask:
			reducer(&task, reducef)
		case WaitTask:
			time.Sleep(time.Second)
			ReportTaskToCoordinator(&task)
		case ExitTask:
			ReportTaskToCoordinator(&task)
			return
		default:
			fmt.Println("unknown task type")
		}
	}

}

// getTaskFromCoordinator 从Coordinator 轮询获取任务
func getTaskFromCoordinator() Task {
	args := TaskArgs{}
	task := Task{}
	ok := call("Coordinator.GetTask", &args, &task)
	if ok {
		// fmt.Printf("get task: %v\n", task.TaskType)
	} else {
		fmt.Println("getTaskFromCoordinator failed")
	}
	return task
}
func ReportTaskToCoordinator(task *Task) {
	args := TaskArgs{}
	// fmt.Printf("report task: %v\n", task.TaskType)
	call("Coordinator.GetReportTask", &args, task)
}
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// 从 input 转化为中间键值对
	intermediate := make([][]KeyValue, task.NReduce)

	filename := task.InputFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// 将中间键写入到 NReduce 个桶中
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// 写出中间文件
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.MapTaskId, i)
		ofile, _ := os.Create(oname)
		defer ofile.Close()

		//  用 json 格式写入对应的key-value对
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}

		// 记录中间文件名
		task.IntermediateFiles = append(task.IntermediateFiles, oname)
	}
	// log.Println("map task: ", task.MapTaskId, " done ")
	ReportTaskToCoordinator(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	for reuduceId := 0; reuduceId < task.NReduce; reuduceId++ {
		// mr-out-X 的键值对
		kva := []KeyValue{}

		for mapid := 0; mapid < task.NMap; mapid++ {
			oname := fmt.Sprintf("mr-%d-%d", mapid, reuduceId)
			file, _ := os.Open(oname)
			defer file.Close()

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		// 排序所有的键值对
		sort.Sort(ByKey(kva))

		// 合并相邻键值对，输出到 mr-out-X 中
		oname := fmt.Sprintf("mr-out-%d", reuduceId)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		// log.Println("reduce task: ", oname, " done ")
		ofile.Close()
	}
	// log.Println("all reduce task done", task.TaskType) // 1
	ReportTaskToCoordinator(task)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
		log.Printf("call success")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("call filed: ", err)
	return false
}
