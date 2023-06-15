package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce       int             // Number of reduce tasks
	Files           []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks        chan MapTask    // Channel for uncompleted map tasks
	CompletedTasks  map[string]bool // Map to check if task is completed
	Lock            sync.Mutex      // Lock for contolling shared variables
	ReduceTasks     chan ReduceTask //Channel for uncompleted reduce tasks
	CompletedReduce map[int]bool    //Map to check if reduce task is complete
	MapComplete     bool            //boolean to see if map tasks are complete
}

// helper function that checks if all map tasks have been completed
func (c *Coordinator) CheckMapComplete() bool {
	for i := range c.CompletedTasks {
		if !c.CompletedTasks[i] {
			return false
		}
	}

	return true
}

func (c *Coordinator) CheckReduceComplete() bool {
	for i := range c.CompletedReduce {
		if !c.CompletedReduce[i] {
			return false
		}
	}
	return true
}

// helper function to create the reduce tasks and put them in the channel
func (c *Coordinator) CreateReduce() {
	for i := 0; i < c.NumReduce; i++ {
		redTask := ReduceTask{
			Bucket:   i,
			TaskType: 2,
		}

		//fmt.Println("ReduceTask", redTask, "added to channel")

		c.ReduceTasks <- redTask
		c.CompletedReduce[redTask.Bucket] = false
	}

}

// Starting coordinator logic
func (c *Coordinator) Start() {
	//fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks and add them to the queue
	count := 0
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:   file,
			FileNumber: count,
			NumReduce:  c.NumReduce,
			TaskType:   1,
		}
		count += 1

		//fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	c.server()
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	//fmt.Println("Map task requested")
	if c.MapComplete {
		//tells worker to move to reduce phase
		reply.Filename = ""
		reply.NumReduce = c.NumReduce
		reply.TaskType = -1
		reply.FileNumber = -1
	} else {

		select {
		case task := <-c.MapTasks: // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt
			//fmt.Println("Map task found,", task.Filename)
			*reply = task

			go c.WaitForWorker(task)
		default:
			//fmt.Println("No map tasks found. Try again later")
			reply.Filename = ""
			reply.NumReduce = c.NumReduce
			reply.TaskType = 0
			reply.FileNumber = -1
		}
	}

	return nil
}

func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {
	//fmt.Println("Reduce task requested")
	reduce_tasks_left := false
	c.Lock.Lock()
	defer c.Lock.Unlock()
	for _, i := range c.CompletedReduce {
		if !i {
			reduce_tasks_left = true
			break
		}
	}

	if reduce_tasks_left {

		select {
		case task := <-c.ReduceTasks: // check if there are uncompleted reduce tasks. Keep in mind, if MapTasks is empty, this will halt
			//fmt.Println("Reduce task found,", task.Bucket)
			//fmt.Println("Sending task", task)
			reply.Bucket = task.Bucket
			reply.TaskType = task.TaskType

			go c.WaitForWorkerReduce(*reply)

		default:
			//fmt.Println("No reduce tasks found. Try again later")
			reply.Bucket = -1
			reply.TaskType = 0
		}
	} else {
		//worker terminate
		reply.Bucket = -1
		reply.TaskType = -1
	}

	return nil
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	//fmt.Println("Locking...WFW")
	c.Lock.Lock()
	if !c.CompletedTasks["map_"+task.Filename] {
		//fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
	//fmt.Println("Unlocked WFW")
}

func (c *Coordinator) WaitForWorkerReduce(task ReduceTask) {
	time.Sleep(time.Second * 10)
	//fmt.Println("Locking... WFWR")
	c.Lock.Lock()
	if !c.CompletedReduce[task.Bucket] {
		//fmt.Println("Timer expired, task", task.Bucket, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
	// fmt.Println("Unlocked WFWR")
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *EmptyReply) error {
	//fmt.Println("Locking... Task Completed")
	c.Lock.Lock()
	defer c.Lock.Unlock()

	//if this task has not already been completed:
	if !c.CompletedTasks["map_"+args.Filename] {

		c.CompletedTasks["map_"+args.Filename] = true

		//fmt.Println("Task", args, "completed")

		// If all of map tasks are completed, go to reduce phase
		// ...
		if c.CheckMapComplete() {
			c.MapComplete = true
			//	fmt.Println("All map tasks complete. Beginning reduce phase")
			c.CreateReduce()

		}
	} else {
		//fmt.Println("Map task", args.Filename, "has already been completed")
	}
	//fmt.Println("Unlocked Task Completed")
	return nil
}

func (c *Coordinator) RedTaskCompleted(args *ReduceTask, reply *EmptyReply) error {
	//fmt.Println("Locking... RedTaskCompleted")
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if !c.CompletedReduce[args.Bucket] {
		//if this is the first time the task has been completed
		c.CompletedReduce[args.Bucket] = true

		//fmt.Println("Task", args, "completed")
	} else {
		//fmt.Println("Task", args.Bucket, "has already been completed")
	}

	//fmt.Println("Unlocked RedTaskCompleted")
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
	c.Lock.Lock()
	//fmt.Println("Locking ... Done")
	defer c.Lock.Unlock()
	ret := false

	// Your code here.
	if c.MapComplete {
		if c.CheckReduceComplete() {
			ret = true
			//fmt.Println("Done completed. Killing Coordinator")
		}

	} else {
		//fmt.Println("Done not done yet")
	}
	time.Sleep(3 * time.Second)
	//fmt.Println("Unlocked Done")

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:       nReduce,
		Files:           files,
		MapTasks:        make(chan MapTask, 100),
		CompletedTasks:  make(map[string]bool),
		MapComplete:     false,
		ReduceTasks:     make(chan ReduceTask, 100),
		CompletedReduce: make(map[int]bool),
	}

	//fmt.Println("Starting coordinator")

	c.Start()

	return &c
}
