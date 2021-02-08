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
	noTasksAvailable = iota
	mapTask
	reduceTask
)

const (
	_ = iota
	needsTask
	intermediateProgress
	done
)

type WorkerMessage struct {
	Id    int
	State int // [needsTask, intermediateProgress, or done]
}

type MasterReply struct {
	TaskType int // [mapTask, or reduceTask]
	TaskNum  int
	FileName string // file name in case of map task*/
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
