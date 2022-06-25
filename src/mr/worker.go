package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ihash
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ByKey 为了实现sort方法
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker
// 执行 map ｜ reduce 任务
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {

	for {
		// RPC
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.DispatchTask", &args, &reply)
		args.Task = reply.Task
		if !ok {
			fmt.Printf("RPC 请求失败!\n")
			break
		}

		switch reply.Mode {
		case Map:
			//fmt.Printf("执行map任务 %v\n", reply.Task.TaskName)
			WordCountMap(reply.Task, mapFunc)
			call("Coordinator.TaskDone", &args, &reply)
		case Reduce:
			//fmt.Printf("执行reduce任务 %v\n", reply.Task.TaskName)
			WordCountReduce(reply.Task, reduceFunc)
			call("Coordinator.TaskDone", &args, &reply)
		case Wait:
			//fmt.Println("休息一些")
			time.Sleep(reply.WakeAfter)
		case Done:
			fmt.Printf("无任务，结束worker")
			return
		}
	}
}

func WordCountMap(task Task, mapFunc func(string, string) []KeyValue) {
	// map任务需要filename,nReduce,id(对应第几个map)
	s := task.Filename
	file, err := os.Open(s)
	if err != nil {
		log.Fatalf("cannot open %v", s)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", s)
	}
	file.Close()
	kva := mapFunc(s, string(content))

	intermediate := make([][]KeyValue, task.N)
	for i := 0; i < task.N; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		y := ihash(kv.Key)%task.N + 1
		intermediate[y-1] = append(intermediate[y-1], kv)
	}

	for i := 0; i < task.N; i++ {
		s = "mr-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(i+1)
		outfile, err := ioutil.TempFile(os.TempDir(), "prefix-")
		if err != nil {
			log.Fatalf("无法创建或打开 %v", s)
		}

		enc := json.NewEncoder(outfile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("无法编码 %v", kv)
			}
		}
		os.Rename(outfile.Name(), s)
		os.Remove(outfile.Name())
	}

}

func WordCountReduce(task Task, reduceFunc func(string, []string) string) {
	// reduce任务需要nReduce,nMap
	kva := make([]KeyValue, 0)
	for i := 1; i <= task.M; i++ {
		s := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.ID)
		file, err := os.Open(s)
		if err != nil {
			log.Fatalf("cannot open %v", s)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	intermediate := kva
	sort.Sort(ByKey(intermediate))

	s := "mr-out-" + strconv.Itoa(task.ID)
	outfile, err := ioutil.TempFile(os.TempDir(), "prefix-")
	if err != nil {
		log.Fatalf("无法创建或打开 %v", s)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	os.Rename(outfile.Name(), s)
	os.Remove(outfile.Name())
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
