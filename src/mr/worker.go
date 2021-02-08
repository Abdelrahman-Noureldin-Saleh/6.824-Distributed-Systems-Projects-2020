package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the GetTask RPC to the master.

	reply := CallMaster(nil)

	for {
		var files []string
		task := reply.Task
		printTask(task)
		switch task.TaskType {
		case mapTask:
			var intermediate []KeyValue
			file, err := os.Open(task.Input[0])
			if err != nil {
				log.Fatalf("cannot open %v", task.Input[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Input[0])
			}
			if err := file.Close(); err != nil {
				log.Fatalf("cannot close %v", task.Input[0])
			}
			kva := mapf(task.Input[0], string(content))
			intermediate = append(intermediate, kva...)
			files = writeIntermediate(intermediate, task.TaskId, reply.NReduce)
		case reduceTask:
			fmt.Printf("reduce... \n")
		default:
			// sleep for one second, then call the master again for a Task
			// the reason for this is to differentiate between when the job is done
			// and when the job is not done, but there are no idle Task
			// we would like to keep this worker alive if there are in-progress tasks
			// in case one of the workers that are working on a Task have died
			// in that case, after a while, this worker will call the master asking for a Task
			// and the master would give it the previously in-progress (now idle) failed Task.
			// if the master didn't reply at all, which indicates either the job is done or
			// the master had died, then and only then this worker will exit.
			time.Sleep(time.Second)
		}
		reply = CallMaster(files)
	}
}

// writes intermediate data to Files
func writeIntermediate(intermediate []KeyValue, mapTaskNum int, nReduce int) []string {
	fmt.Printf("map task num: %d\n", mapTaskNum)
	groups := make(map[int][]KeyValue)
	filesNames := make([]string, 0)
	for _, item := range intermediate {

		hash := ihash(item.Key) % nReduce
		groups[hash] = append(groups[hash], item)
	}
	for key, val := range groups {
		reduceTaskNum := key
		filename := createFileName(mapTaskNum, reduceTaskNum)
		//
		tempFileName := fmt.Sprintf("%s-%d.tmp", filename, os.Getuid())
		finalFileName := fmt.Sprintf("%s.txt", filename)
		f, err := os.OpenFile(tempFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		for _, keyValue := range val {
			if _, err := f.WriteString(fmt.Sprintf("%v %v\n", keyValue.Key, keyValue.Value)); err != nil {
				log.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.Rename(tempFileName, finalFileName); err != nil {
			log.Fatal(err)
		}
		filesNames = append(filesNames, filename)
	}
	return filesNames
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMaster(files []string) MasterReply {

	// declare an argument structure.
	reply := MasterReply{}

	args := WorkerMessage{
		workerId: os.Getuid(),
		Files:    files,
	}

	// send the RPC request, wait for the reply.
	if !call("Master.GetTask", &args, &reply) {
		os.Exit(1)
	}
	printTask(reply.Task)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
