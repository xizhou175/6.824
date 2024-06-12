package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
    MapAssigned []bool
    MapFinished []bool
    ReduceAssigned []bool
    ReduceFinished []bool
    NReduce int
}

type TaskRequest struct {
    // 1 is map, 2 is reduct
    TaskType int
}

type TaskReply struct {
    // 1 is map, 2 is reduce
    TaskType int
    TaskId int           // task number
    FileName string
    NReduce int
    Finished bool    // map/reduce phase is finished or not
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
