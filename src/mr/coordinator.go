package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Mode int

const (
	Map    Mode = 1
	Reduce Mode = 2
	Wait   Mode = 3
	Done   Mode = 4
)

type Coordinator struct {
	Mu              sync.Mutex
	N               int
	M               int
	Phase           Mode
	TasksReady      map[string]Task
	TasksInProgress map[string]Task
}

type Task struct {
	ID       int
	N        int
	M        int
	TaskName string
	Filename string
	StartAt  time.Time
}

// DispatchTask Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchTask(args *Args, reply *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.Phase == Done {
		return nil
	}

	if len(c.TasksReady) > 0 {
		for k, v := range c.TasksReady {
			fmt.Printf("发布任务 %v \n", v.TaskName)
			v.StartAt = time.Now()
			*reply = Reply{
				Mode: c.Phase,
				Task: v,
			}

			delete(c.TasksReady, k)
			c.TasksInProgress[k] = v
			return nil
		}

	} else if len(c.TasksInProgress) > 0 {
		// 将失败任务重新收集发布
		c.CollectFailTask()

		*reply = Reply{
			Mode:      Wait,
			WakeAfter: 1 * time.Second,
		}

	} else if c.Phase == Map {
		// 准备reduce任务并切换coordinator的阶段状态
		fmt.Println("准备Reduce任务")
		c.MakeReduceTasks()
		c.Phase = Reduce

		*reply = Reply{
			Mode:      Wait,
			WakeAfter: 1 * time.Second,
		}

	} else {
		*reply = Reply{
			Mode: Done,
		}
		c.Phase = Done
	}

	return nil
}

func (c *Coordinator) CollectFailTask() {
	if len(c.TasksInProgress) > 0 {
		for k, v := range c.TasksInProgress {
			if time.Now().Sub(v.StartAt) > 10*time.Second {
				fmt.Printf("重新收集任务 %v\n", k)
				delete(c.TasksInProgress, k)
				c.TasksReady[k] = v
			}
		}
	}
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	s := args.Task.TaskName
	if _, ok := c.TasksInProgress[s]; ok {
		fmt.Printf("%v 任务已经完成\n", s)
		delete(c.TasksInProgress, s)
	}

	return nil
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	if c.Phase == Done {
		return true
	}

	return false
}

// MakeCoordinator
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.M = len(files)
	c.N = nReduce
	c.Phase = Map
	c.TasksReady = make(map[string]Task)
	c.TasksInProgress = make(map[string]Task)

	c.MakeMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) MakeMapTasks(files []string) {
	for i, file := range files {
		t := Task{
			ID:       i + 1,
			N:        c.N,
			M:        c.M,
			TaskName: GenerateTaskName(i+1, Map),
			Filename: file,
		}
		c.TasksReady[t.TaskName] = t
	}
}

func (c *Coordinator) MakeReduceTasks() {
	for i := 1; i <= c.N; i++ {
		t := Task{
			ID:       i,
			N:        c.N,
			M:        c.M,
			TaskName: GenerateTaskName(i, Reduce),
		}
		c.TasksReady[t.TaskName] = t
	}
}

func GenerateTaskName(id int, mode Mode) string {
	if mode == Map {
		return fmt.Sprintf("map-task-%v", id)
	} else if mode == Reduce {
		return fmt.Sprintf("reduce-task-%v", id)
	} else {
		fmt.Println("输入参数Mode有误")
		return "error"
	}
}
