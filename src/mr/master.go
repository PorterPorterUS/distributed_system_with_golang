package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	mapTask    = 1
	reduceTask = 2
)

//state for task
const (
	initialState = 0
	inProgress   = 1
	complete     = 2
)

//state for master
const (
	newMaster = iota
	completeMap
	completeReduce
)

type Master struct {
	// Your definitions here.
	nReduce          int
	masterState      int
	mapTask          []Task
	reduceTask       []Task
	intermediateFile map[int][]string
	end              bool
}

type Task struct {
	Type_    int
	Id       int
	Filename string
	State    int
	NReduce  int
	Files    int
	Time     time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetTask(_ *ExampleArgs, reply *GetTaskReply) error {
	switch m.masterState {
	case newMaster:
		for i, task := range m.mapTask {
			if task.State == initialState {
				reply.Task.Type_ = task.Type_
				reply.Task.Filename = task.Filename
				reply.Task.Id = task.Id
				reply.Task.NReduce = task.NReduce
				reply.Flag = 0
				reply.Task.State = task.State

				m.mapTask[i].State = inProgress
				m.mapTask[i].Time = time.Now()
				//reply.Task.State=m.mapTask[i].State

				return nil
			} else if task.State == inProgress && time.Now().Sub(m.mapTask[i].Time) > time.Duration(5)*time.Second {
				reply.Task.Type_ = task.Type_
				reply.Task.Filename = task.Filename
				reply.Task.Id = task.Id
				reply.Task.NReduce = task.NReduce
				reply.Task.State = task.State
				reply.Flag = 0

				m.mapTask[i].State = inProgress
				m.mapTask[i].Time = time.Now()
				//reply.Task.State=m.mapTask[i].State

				return nil
			}
		}
		reply.Flag = 1 // map not finished but in progress
	case completeMap:
		for i, task := range m.reduceTask {
			if task.State == initialState {
				reply.Task.Type_ = task.Type_
				reply.Task.Filename = task.Filename
				reply.Task.Id = task.Id
				reply.Task.NReduce = task.NReduce
				reply.Flag = 0
				reply.Task.Files = task.Files
				reply.Task.State = task.State

				m.reduceTask[i].State = inProgress
				m.reduceTask[i].Time = time.Now()
				//reply.Task.State=m.mapTask[i].State

				return nil
			} else if task.State == inProgress && time.Now().Sub(m.reduceTask[i].Time) > time.Duration(5)*time.Second {
				reply.Task.Type_ = task.Type_
				reply.Task.Filename = task.Filename
				reply.Task.Id = task.Id
				reply.Task.NReduce = task.NReduce
				reply.Flag = 0
				reply.Task.Files = task.Files
				reply.Task.State = task.State

				m.reduceTask[i].State = inProgress
				m.reduceTask[i].Time = time.Now()
				//reply.Task.State=m.mapTask[i].State

				return nil

			}
		}
		reply.Flag = 1 // reduce not finished but in progress
	case completeReduce:
		reply.Flag = 2 // all task have been finished

	}

	return nil
}

func (m *Master) TaskCompare(args *TaskCompareArgs, reply *ExampleReply) error {
	if args.Type == mapTask {
		for i, task := range m.mapTask {
			if task.Id == args.ID {
				m.mapTask[i].State = complete
			}
		}
	} else {
		for i, task := range m.reduceTask {
			if task.Id == args.ID {
				m.reduceTask[i].State = complete
			}
		}
	}
	m.checkTasksComplete()
	return nil
}

func (m *Master) checkTasksComplete() {
	switch m.masterState {
	case newMaster:
		for _, j := range m.mapTask {
			if j.State != complete {
				return
			}
		}
		m.masterState = completeMap
		//start creating reduce task
		for i := 0; i < m.nReduce; i++ {
			m.reduceTask = append(m.reduceTask, Task{
				Type_:    reduceTask,
				Id:       i,
				Filename: "",
				State:    initialState,
				NReduce:  m.nReduce,
				Files:    len(m.mapTask),
			})

		}
	case completeMap:
		for _, j := range m.reduceTask {
			if j.State != complete {
				return
			}
		}
		m.masterState = completeReduce
		m.end = true
	case completeReduce:
		m.end = true

	default:

	}

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.

	return m.end
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:     nReduce,
		mapTask:     []Task{},
		masterState: newMaster,
		end:         false,
	}

	// Your code here.

	for i, file := range files {
		m.mapTask = append(m.mapTask, Task{
			Type_:    mapTask,
			Id:       i,
			Filename: file,
			State:    initialState,
			NReduce:  m.nReduce,
		})
	}

	go m.server()
	return &m
}
