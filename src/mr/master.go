package mr

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//

type Master struct {
	mutex sync.Mutex

	tasks       PriorityQueue
	assignments map[int]*Task

	nMap                  int        // number of map tasks
	nReduce               int        // number of reduce tasks
	intermediateFileNames [][]string // intermediateFileNames, rows are maps, columns are reduce
	outputFileNames       []string   // outputFileNames, each row is for a different reduce function
	// example: intermediateFileNames[mapIdx][reduceIdx] == createIntermediateFileName(mapIdx, reduceTaskNum)
	// should be 'true' if the map function with mapIdx finished execution
}

const (
	_ = iota
	idle
	inProgress
	completed
)

// maximum number of workers that can be assigned a task simultaneously
const replicas int = 3

// this priority queue works as the tasks scheduler.
// modifying Less(i, j int) function modifies the order of tasks execution
type PriorityQueue []*Task

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	// does pq[i] have lower priority than pq[j]

	if pq[i].TaskStatus == completed {
		return false
	}

	if pq[j].TaskStatus == completed {
		return true
	}

	// map tasks have priority over reduce tasks
	if pq[i].TaskType != pq[j].TaskType {
		return pq[i].TaskType < pq[j].TaskType
	}

	// otherwise the status of the Task takes order (idle -> in-progress -> completed)
	if pq[i].TaskStatus != pq[j].TaskStatus {
		return pq[i].TaskStatus < pq[j].TaskStatus
	}

	if len(pq[i].WorkersIDs) != len(pq[j].WorkersIDs) {
		return len(pq[i].WorkersIDs) < len(pq[j].WorkersIDs)
	}

	// otherwise the number of Task takes the order (mapTask 1 should be executed before mapTask 2)
	return pq[i].TaskId < pq[j].TaskId
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Task)
	item.Index = n

	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}
func (pq *PriorityQueue) Peek() interface{} {
	return (*pq)[0]
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Task, updated *Task) {
	item.WorkersIDs = updated.WorkersIDs
	item.TaskStatus = updated.TaskStatus
	item.TaskType = updated.TaskType
	item.TaskId = updated.TaskId
	item.Input = updated.Input
	item.Index = updated.Index
	heap.Fix(pq, item.Index)
}

func (pq *PriorityQueue) fix(item *Task) {
	heap.Fix(pq, item.Index)
}

type Task struct {
	// Index of this Task, used for PriorityQueue
	Index int

	// a "set" of IDs of the worker(s) working on this Task
	WorkersIDs map[int]struct{}

	// the status of the Task, one of [idle, in-progress, completed]
	TaskStatus int

	// the type of the Task, one of [mapTask, reduceTask]
	TaskType int

	// the number of the Task, each Task should have a unique pair of (TaskType, TaskId)
	TaskId int

	// Input Files for this Task, a single file in case of map tasks, a list of Files in case of reduce Task
	Input []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (master *Master) GetTask(args *WorkerMessage, reply *MasterReply) error {
	//fmt.Printf("# getTask call with packet => %v\n", *args)

	// if the message contains output file(s), they file names/paths should be saved
	if args.Files != nil {
		master.mutex.Lock()
		master.saveResult(args)
		master.mutex.Unlock()
	}

	// assign task to worker
	if task, ok := master.tasks.Peek().(*Task); ok {
		switch task.TaskStatus {

		case completed:
			reply.Task = Task{TaskType: thanks}

		default:
			if len(task.WorkersIDs) < replicas {
				master.mutex.Lock()
				master.assignTask(task, args, reply)
				master.mutex.Unlock()
			}
		}
	}
	return nil
}

func (master *Master) assignTask(task *Task, args *WorkerMessage, reply *MasterReply) {
	task.TaskStatus = inProgress
	task.WorkersIDs[args.WorkerId] = struct{}{}
	master.assignments[args.WorkerId] = task
	if task.TaskType == reduceTask {
		if len(task.Input) == 0 {
			for _, row := range master.intermediateFileNames {
				task.Input = append(task.Input, row[task.TaskId])
			}
		}
	}
	reply.Task = *task
	reply.NReduce = master.nReduce
	reply.NMap = master.nMap
	master.tasks.fix(task)
	time.AfterFunc(time.Second*10, func() {
		master.mutex.Lock()
		if task.TaskStatus != completed {
			delete(task.WorkersIDs, args.WorkerId)
			if len(task.WorkersIDs) == 0 {
				task.TaskStatus = idle
			}
			master.tasks.fix(task)
		}
		master.mutex.Unlock()
	})
}

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
	return master.tasks.Peek().(*Task).TaskStatus == completed
}

func (master *Master) saveResult(args *WorkerMessage) {
	task := master.assignments[args.WorkerId]
	delete(task.WorkersIDs, args.WorkerId)
	if task.TaskStatus != completed {
		switch task.TaskType {
		case mapTask:
			master.intermediateFileNames[task.TaskId] = args.Files
		case reduceTask:
			master.outputFileNames[task.TaskId] = args.Files[0]
		}
		task.TaskStatus = completed
		master.tasks.fix(task)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	master := Master{
		tasks: make(PriorityQueue, 0),
		nMap:  nMap, nReduce: nReduce,
		assignments:           make(map[int]*Task),
		intermediateFileNames: make([][]string, nMap),
		outputFileNames:       make([]string, nReduce),
	}
	heap.Init(&master.tasks)

	// creating mapTasks
	for idx, file := range files {
		heap.Push(&master.tasks, &Task{
			TaskStatus: idle, TaskType: mapTask, TaskId: idx,
			WorkersIDs: map[int]struct{}{}, Input: []string{file},
		})
	}

	// creating reduce tasks
	for idx := 0; idx < nReduce; idx++ {
		heap.Push(&master.tasks, &Task{
			TaskStatus: idle, TaskType: reduceTask, TaskId: idx,
			WorkersIDs: make(map[int]struct{}), Input: []string{},
		})
	}

	/*var tasks []*Task
	for len(master.tasks) != 0 {
		task := heap.Pop(&master.tasks).(*Task)
		printTask(*task)
		tasks = append(tasks, task)
	}

	for _, task := range tasks {
		heap.Push(&master.tasks, task)
	}*/

	// listen to workers
	master.server()
	return &master
}

func createIntermediateFileName(mapTaskNum int, ReduceTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNum, ReduceTaskNum)
}

func createFinalFileName(reduceTaskNum int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskNum)
}

func printTask(task Task) {
	status := map[int]string{completed: "Completed", idle: "idle", inProgress: "inProgress"}
	taskType := map[int]string{mapTask: "mapTask", reduceTask: "redTask", thanks: "thanks"}
	fmt.Printf("{index:%d, workerId:%d, taskType:%s, taskStatus:%s, TaskId:%d, iuput:%v}\n", task.Index, task.WorkersIDs, taskType[task.TaskType], status[task.TaskStatus], task.TaskId, task.Input)
}
