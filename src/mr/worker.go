package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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
	var tasktype string
	var filename string
	var index int
	var reduce_intermediate []KeyValue
	var nReduce int

	GetReduceNumber(&nReduce)
	AskforTask(&filename, &reduce_intermediate, &tasktype, &index)

	for filename != "All done" {
		if tasktype == "Map" {
			intermediate := [][]KeyValue{}
			for i := 0; i < nReduce; i++ {
				intermediate = append(intermediate, []KeyValue{})
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
			//reduce_bucket := ihash(kva.Key) % 3
			for _, kv := range kva {
				reduce_id := ihash(kv.Key) % nReduce
				intermediate[reduce_id] = append(intermediate[reduce_id], kv)
			}
			SendMapResult(intermediate, index)
			AskforTask(&filename, &reduce_intermediate, &tasktype, &index)
		} else if tasktype == "Reduce" {
			oname := "mr-out-" + strconv.Itoa(index)
			ofile, _ := os.Create(oname)
			sort.Sort(ByKey(reduce_intermediate))
			outputs := map[string]string{}
			i := 0
			for i < len(reduce_intermediate) {
				j := i + 1
				for j < len(reduce_intermediate) && reduce_intermediate[j].Key == reduce_intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reduce_intermediate[k].Value)
				}
				output := reducef(reduce_intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", reduce_intermediate[i].Key, output)
				outputs[reduce_intermediate[i].Key] = output
				i = j
			}

			AskforTask(&filename, &reduce_intermediate, &tasktype, &index)
		}
	}
	//AskForExit()
}

func GetReduceNumber(nReduce *int) {
	args := Args{}
	reply := NReduceReply{}
	ok := call("Coordinator.ReplyReduceNumber", &args, &reply)
	if ok {
		*nReduce = reply.NReduce
	} else {
		fmt.Printf("call ReduceNumber failed\n")
	}

}

// worker call the coordinator for asking for a task via this function
func AskforTask(filename *string, reduce_intermediate *[]KeyValue, tasktype *string, index *int) {
	args := Args{}
	reply := TaskReply{}
	ok := call("Coordinator.ReplyTask", &args, &reply)
	if ok {
		*filename = reply.Filename
		*tasktype = reply.Tasktype
		*index = reply.Index
		*reduce_intermediate = reply.ReduceIntermediate
		//fmt.Printf("Tasktype:%v Filename: %v\n", reply.Tasktype, reply.Filename)
	} else {
		fmt.Printf("call failed\n")
	}
}

func SendMapResult(result [][]KeyValue, index int) {
	args := Args{}
	args.MapResult = result
	args.Index = index
	reply := TaskReply{}
	ok := call("Coordinator.GetMapResult", &args, &reply)
	if !ok {
		fmt.Printf("map result sent failed\n")
	}
}

func AskForExit() {
	args := Args{}
	reply := TaskReply{}
	for {
		ok := call("Coordinator.ReplyEnd", &args, &reply)
		if ok {
			time.Sleep(time.Second)
		} else {
			time.Sleep(time.Second)
			break
		}
	}
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

	fmt.Println(err)
	return false
}
