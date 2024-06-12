package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "strings"
import "encoding/json"
import "path/filepath"
import "time"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
    // ask for map task
    for {
        reply := AskForTask(1)

        filename := reply.FileName

        //fmt.Printf("%v map finished\n", reply.Finished)
        if reply.Finished == true {
            break
        }

        if filename == "" {
            time.Sleep(time.Second)
            continue
        }

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

        intermediate := make([][]KeyValue, reply.NReduce)
        for i := 0; i < len(kva); i++ {
            kv := kva[i]
            bucket := ihash( kv.Key ) % reply.NReduce
            kvs := intermediate[bucket]
            intermediate[bucket] = append(kvs, kv)
        }
        for i := 0; i < reply.NReduce; i++ {
            sort.Sort(ByKey(intermediate[i]))
            X := strconv.Itoa(reply.TaskId)
            Y := strconv.Itoa(i)
            filename := "mr-" + X + "-" + Y
            if _, err := os.Stat("mr-tmp"); err != nil {
                os.Mkdir("mr-tmp", 0777)
            }
            path := filepath.Join("mr-tmp", filename)
            file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                continue
            }
            kvs := intermediate[i]

            enc := json.NewEncoder(file)
            for j := 0; j < len(kvs); j++ {
                kv := kvs[j]
                enc.Encode(&kv)
            }
            file.Close()
        }
        var r int = 0;
        call("Coordinator.Finish", &reply.TaskId, &r)
    }

    // ask for reduce task
    for {
        reply := AskForTask(2)
        //fmt.Printf("reply.TaskId %v\n", reply.TaskId)

        if reply.Finished == true {
            break
        }

        if reply.TaskId == -1 {
            time.Sleep(time.Second)
            continue
        }

        // read all files mr-X-{reply.TaskId}, where X is map task number
        files, err := ioutil.ReadDir("./mr-tmp")
        if err != nil {
            log.Fatal(err)
        }

        intermediate := []KeyValue{}
        filename := "mr-out-" + strconv.Itoa(reply.TaskId)
        path := filepath.Join("./", filename)
        ofile, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
        for _, f := range files {
            index := strings.LastIndex(f.Name(), "-")
            fName := f.Name()
            Y, _ := strconv.Atoi(fName[index+1:])
            if Y != reply.TaskId {
                continue
            }

            ipath := filepath.Join("mr-tmp", fName)
            ifile, err := os.OpenFile(ipath, os.O_RDONLY, 0644)
            if err != nil {
                continue
            }
            dec := json.NewDecoder(ifile)
            for {
                var kv KeyValue
                if err := dec.Decode(&kv); err != nil {
                  break
                }
                intermediate = append(intermediate, kv)
            }
        }
        i := 0
        sort.Sort(ByKey(intermediate))
        for i < len(intermediate) {
            j := i + 1
            for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                j++
            }
            values := []string{}
            for k := i; k < j; k++ {
                values = append(values, intermediate[k].Value)
            }
            output := reducef(intermediate[i].Key, values)

            // this is the correct format for each line of Reduce output.
            fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

            i = j
        }

        ofile.Close()
        var r int = 0;
        call("Coordinator.ReduceFinish", &reply.TaskId, &r)
    }
}

func AskForTask(TaskType int) TaskReply {
    req := TaskRequest{}
    req.TaskType = TaskType

    reply := TaskReply{}
    call("Coordinator.AssignTask", &req, &reply)

	//fmt.Printf("reply.TaskId in AskForTask %v\n", reply.TaskId)
	//fmt.Printf("reply.TaskType in AskForTask %v\n", reply.TaskType)
    return reply
}

//
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}
