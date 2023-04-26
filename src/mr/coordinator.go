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

type End struct {
	mutex sync.Mutex
	end   bool
	phase string
}

type File struct {
	mutex        sync.Mutex
	filename     string
	status       bool
	intermediate [][]KeyValue
}

type Indices struct {
	mutex       sync.Mutex
	mapIndex    int
	reduceIndex int
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []File
	indices Indices
	end     End
}

func (c *Coordinator) InitializeIntermediate() {
	for i := 0; i < len(c.files); i++ {
		c.files[i].intermediate = make([][]KeyValue, c.nReduce)
	}
}

func (c *Coordinator) GetReduceIntermediate(index int) []KeyValue {
	reduce_intermediate := []KeyValue{}
	for i := 0; i < len(c.files); i++ {
		c.files[i].mutex.Lock()
		reduce_intermediate = append(reduce_intermediate, c.files[i].intermediate[index]...)
		c.files[i].mutex.Unlock()
	}
	return reduce_intermediate
}

func (c *Coordinator) ReplyReduceNumber(args *Args, reply *NReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReplyTask(args *Args, reply *TaskReply) error {

	c.indices.mutex.Lock()
	check_mapIndex := (c.indices.mapIndex == -1)
	c.indices.mutex.Unlock()
	if !check_mapIndex {
		for i := 0; i < len(c.files); i++ {
			c.files[i].mutex.Lock()
			status := (c.files[i].status == false)
			c.files[i].mutex.Unlock()
			if status {
				reply.Filename = c.files[i].filename
				reply.Tasktype = "Map"
				c.indices.mutex.Lock()
				reply.Index = c.indices.mapIndex
				c.indices.mutex.Unlock()
				c.files[i].mutex.Lock()
				c.files[i].status = true
				c.files[i].mutex.Unlock()
				c.indices.mutex.Lock()
				c.indices.mapIndex++
				c.indices.mutex.Unlock()
				return nil
				//fmt.Println(c.indices.mapIndex)
			}
		}
		//c.indices_m.mutex.Unlock()
		c.indices.mutex.Lock()
		c.indices.mapIndex = -1
		c.indices.mutex.Unlock()
	}

	c.end.mutex.Lock()
	//check_phase := (c.end.phase == "Reduce")
	c.end.mutex.Unlock()
	c.indices.mutex.Lock()
	reduceIndex := c.indices.reduceIndex
	c.indices.mutex.Unlock()

	//if !check_phase {
	//	return nil
	//} else

	if reduceIndex < c.nReduce {
		reply.Filename = "All maps done"
		reply.Tasktype = "Reduce"
		c.indices.mutex.Lock()
		reply.ReduceIntermediate = c.GetReduceIntermediate(c.indices.reduceIndex)
		reply.Index = reduceIndex
		c.indices.reduceIndex++
		c.indices.mutex.Unlock()
		return nil
	}
	reply.Filename = "All done"
	c.end.mutex.Lock()
	c.end.end = true
	c.end.mutex.Unlock()
	return nil
}

func (c *Coordinator) GetMapResult(args *Args, reply *TaskReply) error {
	c.files[args.Index].mutex.Lock()
	c.files[args.Index].intermediate = args.MapResult
	c.files[args.Index].mutex.Unlock()
	if args.Index == (len(c.files) - 1) {
		c.end.mutex.Lock()
		c.end.phase = "Reduce"
		c.end.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) ReplyEnd(args *Args, reply *TaskReply) error {
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.end.mutex.Lock()
	if c.end.end {
		time.Sleep(time.Second)
		time.Sleep(time.Second)
		time.Sleep(time.Second)
		time.Sleep(time.Second)
		time.Sleep(time.Second)
		c.end.mutex.Unlock()
		return true
	}
	c.end.mutex.Unlock()
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//files_map := map[string]bool{}
	for _, filename := range files {
		file := File{filename: filename, status: false}
		c.files = append(c.files, file)
	}
	c.nReduce = nReduce
	c.indices.mapIndex = 0
	c.indices.reduceIndex = 0
	c.InitializeIntermediate()
	c.end.phase = "Map"
	c.end.end = false
	c.server()
	return &c
}
