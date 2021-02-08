package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//
type Master struct {
	mapTask map[int]map[int]MapTask
	redTask map[int]map[int]RedTask
}

const (
	_ = iota
	idle
	inProgress
	completed
)

type MapTask struct {
	fileName string
	workerId int
}

type RedTask struct {
	workerId int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (master *Master) GetTask(args *WorkerMessage, reply *MasterReply) error {
	switch args.State {
	case needsTask:
		if len(master.mapTask[idle]) != 0 {
			// get first available idle map task
			num, task := getMapTask(master.mapTask[idle])

			//// file the reply message
			reply.TaskType = mapTask
			reply.TaskNum = num
			reply.FileName = task.fileName

			// move task from idle to inProgress
			master.mapTask[inProgress][num] = task
			delete(master.mapTask[idle], num)

		} else if len(master.redTask[idle]) != 0 {
			// get first available idle reduce task
			num, task := getRedTask(master.redTask[idle])

			//// fill the reply message
			reply.TaskType = reduceTask
			reply.TaskNum = num

			// move task from idle to inProgress
			master.redTask[inProgress][num] = task
			delete(master.redTask[idle], num)
		} else {
			reply.TaskType = noTasksAvailable
		}
	}
	return nil
}

func getMapTask(m map[int]MapTask) (int, MapTask) {
	for k := range m {
		return k, m[k]
	}
	return -1, MapTask{}
}

func getRedTask(m map[int]RedTask) (int, RedTask) {
	for k := range m {
		return k, m[k]
	}
	return -1, RedTask{}
}

//
// start a thread that listens for RPCs from worker.go
//
func (master *Master) server() {
	rpc.Register(master)
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
func (master *Master) Done() bool {
	print(len(master.mapTask[idle]), "\t", len(master.redTask[idle]), "\n")
	return len(master.mapTask[idle]) == 0 &&
		len(master.redTask[idle]) == 0 &&
		len(master.mapTask[inProgress]) == 0 &&
		len(master.redTask[inProgress]) == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	master := Master{
		mapTask: make(map[int]map[int]MapTask),
		redTask: make(map[int]map[int]RedTask),
	}

	master.mapTask[idle] = make(map[int]MapTask)
	master.mapTask[inProgress] = make(map[int]MapTask)
	master.mapTask[completed] = make(map[int]MapTask)

	master.redTask[idle] = make(map[int]RedTask)
	master.redTask[inProgress] = make(map[int]RedTask)
	master.redTask[completed] = make(map[int]RedTask)

	// making len(files) idle map tasks
	// [assumes len(files) == number of map tasks]
	for idx, file := range files {
		master.mapTask[idle][idx] = MapTask{
			fileName: file,
			workerId: -1,
		}
	}
	for i := 0; i < nReduce; i++ {
		master.redTask[idle][i] = RedTask{
			workerId: -1,
		}
	}

	master.server()
	return &master
}
