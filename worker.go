package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func make_intermediate(task_num int, numbuckets int) []*os.File {
	intermediate := make([]*os.File, numbuckets)
	for i := 0; i < len(intermediate); i++ {
		oname := "mr-" + strconv.Itoa(task_num) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		intermediate[i] = ofile
	}
	return intermediate
}

func find_reduce_files(reducenum int) []string {
	result, err := filepath.Glob("mr-*-" + strconv.Itoa(reducenum))
	if err != nil {
		log.Fatalf("cannot find any reduce files for bucket", reducenum)
	}
	return result
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}

	w.RequestMapTask()
	w.RequestReduceTask()
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {
	for {
		args := EmptyArs{}
		reply := MapTask{}
		call("Coordinator.RequestMapTask", &args, &reply)
		//fmt.Println("Requesting map task")
		if reply.TaskType == -1 {
			//fmt.Println("Moving worker to reduce phase")
			break

		} else if reply.TaskType == 0 {
			//no tasks available, wait
			//fmt.Println("No map tasks available. Waiting 2 seconds")
			time.Sleep(1 * time.Second)

		} else if reply.TaskType == 1 {

			//given map task, execute task
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			kva := w.mapf(reply.Filename, string(content))

			// store kva in multiple files according to rules described in the README
			// ...
			intermediate := make_intermediate(reply.FileNumber, reply.NumReduce)

			for _, s := range kva {
				var bucketnum int = ihash(s.Key) % reply.NumReduce
				//write to filename using bucket num
				ofile := intermediate[bucketnum]
				buffer := bufio.NewWriter(ofile)
				enc := json.NewEncoder(buffer)
				err := enc.Encode(s)
				if err != nil {
					log.Fatalf("cannot write to file %v", ofile)
				}
				buffer.Flush()
			}

			//fmt.Println("Map task for", reply.Filename, "completed")
			//fmt.Println(kva)

			emptyReply := EmptyReply{}
			call("Coordinator.TaskCompleted", &reply, &emptyReply)
		}
		time.Sleep(1 * time.Second)

	}
}

func (w *WorkerSt) RequestReduceTask() {
	//fmt.Println("Starting Reduce phase")
	for {
		args := EmptyArs{}
		reply := ReduceTask{}
		//fmt.Println("Requesting reduce task")
		call("Coordinator.RequestReduceTask", &args, &reply)
		if reply.TaskType == -1 {
			//fmt.Println("Reduce complete. Killing worker")
			break
		} else if reply.TaskType == 0 {
			//no tasks available, wait
			//fmt.Println("No reduce tasks available. Waiting 2 seconds")
			time.Sleep(2 * time.Second)

		} else if reply.TaskType == 2 {
			//fmt.Println("Received reduce task", reply)
			reduce_list := find_reduce_files(reply.Bucket)
			var kva []KeyValue
			//fmt.Println("beginning reduce bucket", reply.Bucket)
			//fmt.Println("files to reduce:", reduce_list)

			for i := 0; i < len(reduce_list); i++ {

				file, err := os.Open(reduce_list[i])
				if err != nil {
					log.Fatalf("cannot open %v", reduce_list[i])
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(reply.Bucket)
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
				output := w.reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			//fmt.Printf("Writing to output file...")

			ofile.Close()

			//fmt.Println("Reduce task for", reply.Bucket, "completed")

			emptyReply := EmptyReply{}
			call("Coordinator.RedTaskCompleted", &reply, &emptyReply)

		}
		time.Sleep(1 * time.Second)
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

	//fmt.Println(err)
	return false
}
