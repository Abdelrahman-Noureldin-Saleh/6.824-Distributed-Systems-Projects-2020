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

const (
	err = iota
	mapTask
	reduceTask

	// pseudo-task, tells the worker it no longer needs to stay alive, and the job is done
	// this is returned in case the worker contacted the master between the time the master
	// acknowledges that all the tasks are done, but it didn't exit yet
	// Note: if this is not used, and alternatively the master returns a nil task or an empty task
	//       then the worker will wait, and the next time the worker calls the master, the master
	// 		 will be dead, and the worker would know to exit, so this just simply lets the worker dies
	// 		 early.
	thanks
)

type WorkerMessage struct {
	WorkerId int
	Files    []string
}

type MasterReply struct {
	Task    Task
	NReduce int
	NMap    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
