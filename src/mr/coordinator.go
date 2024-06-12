package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
    tasks Task
    files map[int]string
    mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(req *TaskRequest, reply *TaskReply) error {
    reply.TaskType =  req.TaskType

    c.mu.Lock()
    if req.TaskType == 1 {
        MapNum := -1
        for i := 0; i < len(c.tasks.MapAssigned); i++ {
            if c.tasks.MapAssigned[i] == false {
                reply.Finished = false
                MapNum = i
                reply.Finished = false
                c.tasks.MapAssigned[i] = true
                break
            }
        }

        //fmt.Printf("%v", MapNum)
        if MapNum != -1 {
            f := c.files[MapNum]
            reply.FileName = f
            reply.TaskId = MapNum
            reply.NReduce = c.tasks.NReduce
            go c.WaitForTask(req.TaskType, reply.TaskId)
        } else {
            reply.Finished = true
            for i := 0; i < len(c.tasks.MapFinished); i++ {
                if c.tasks.MapFinished[i] == false {
                    reply.Finished = false
                    break
                }
            }
            reply.FileName = ""
        }
    } else {
        ReduceNum := -1
        for i := 0; i < len(c.tasks.ReduceAssigned); i++ {
            if c.tasks.ReduceAssigned[i] == false {
                reply.Finished = false
                ReduceNum = i
                reply.Finished = false
                c.tasks.ReduceAssigned[i] = true
                break
            }
        }

        //fmt.Printf("%v", MapNum)
        reply.TaskId = ReduceNum
        if ReduceNum != -1 {
            go c.WaitForTask(req.TaskType, ReduceNum)
        } else {
            reply.Finished = true
            for i := 0; i < len(c.tasks.ReduceFinished); i++ {
                //fmt.Printf("ReduceFinished[%v]: %v\n", i, c.tasks.ReduceFinished[i])
                if c.tasks.ReduceFinished[i] == false {
                    reply.Finished = false
                    break
                }
            }
        }
    }

    c.mu.Unlock()
	return nil
}

func (c *Coordinator) Finish(X *int, reply *int) error {
    c.mu.Lock()
    c.tasks.MapFinished[*X] = true

    fmt.Printf("Map task %v finished\n", *X)
    c.mu.Unlock()
    return nil
}

func (c *Coordinator) ReduceFinish(X *int, reply *int) error {
    c.mu.Lock()
    c.tasks.ReduceFinished[*X] = true

    fmt.Printf("Reduce task %v finished\n", *X)
    c.mu.Unlock()
    return nil
}

func (c *Coordinator) WaitForTask(TaskType int, TaskNum int) {
	<-time.After(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
    if TaskType == 1 {
        if c.tasks.MapFinished[TaskNum] == false {
            c.tasks.MapAssigned[TaskNum] = false
        }
    } else {
        if c.tasks.ReduceFinished[TaskNum] == false {
            c.tasks.ReduceAssigned[TaskNum] = false
        }
    }
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    c.mu.Lock()
	ret := true

	// Your code here.

    for i := 0; i < len(c.tasks.ReduceFinished); i++ {
        if c.tasks.ReduceFinished[i] == false {
            ret = false
            break
        }
    }

    c.mu.Unlock()
    if ret == true {
        os.RemoveAll("./mr-tmp")
    }

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

    fmt.Printf("MakeCoordinator: nReduce %v\n", nReduce)
    c := Coordinator{files: make(map[int]string)}
    c.tasks = Task{NReduce: nReduce}
    for i := 0; i < len(files); i++ {
        c.files[i] = files[i]
        c.tasks.MapAssigned = append(c.tasks.MapAssigned, false)
        c.tasks.MapFinished =  append(c.tasks.MapFinished, false)
    }
    for i := 0; i < nReduce; i++ {
        c.tasks.ReduceAssigned = append(c.tasks.ReduceAssigned, false)
        c.tasks.ReduceFinished = append(c.tasks.ReduceFinished, false)
    }

	// Your code here.

	c.server()
	return &c
}
