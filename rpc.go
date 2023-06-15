package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyArs struct {
}

type EmptyReply struct {
}

// Universal Task structure
type MapTask struct {
	Filename   string // Filename = key
	FileNumber int
	NumReduce  int // Number of reduce tasks, used to figure out number of buckets
	TaskType   int //0 means no task available, 1 means map task, -1 means map completed
}

type ReduceTask struct {
	Bucket   int
	TaskType int //0 means no task available, 2 means reduce task, -1 means reduce completed
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
