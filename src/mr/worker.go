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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the GetTask RPC to the master.
	reply := CallMaster(nil)

	for {
		var files []string
		switch reply.TaskType {
		case mapTask:
			var intermediate []KeyValue
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			if err := file.Close(); err != nil {
				log.Fatalf("cannot close %v", reply.FileName)
			}
			kva := mapf(reply.FileName, string(content))
			intermediate = append(intermediate, kva...)
			files = writeIntermediate(intermediate, reply.TaskNum, reply.nMap, reply.nReduce)
		case reduceTask:

		case noTasksAvailable:
			// sleep for one second, then call the master again for a task
			// the reason for this is to differentiate between when the job is done
			// and when the job is not done, but there are no idle task
			// we would like to keep this worker alive if there are in-progress tasks
			// in case one of the workers that are working on a task have died
			// in that case, after a while, this worker will call the master asking for a task
			// and the master would give it the previously in-progress (now idle) failed task.
			// if the master didn't reply at all, which indicates either the job is done or
			// the master had died, then and only then this worker will exit.
			time.Sleep(time.Second)
		}
		CallMaster(files)
	}
}

// writes intermediate data to files
func writeIntermediate(intermediate []KeyValue, mapTaskNum int, nMap, nReduce int) []string {
	groups := make(map[int][]KeyValue)
	filesNames := make([]string, nMap)
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
		Id:    os.Getuid(),
		State: needsTask,
		files: files,
	}

	// send the RPC request, wait for the reply.
	if !call("Master.GetTask", &args, &reply) {
		os.Exit(1)
	}

	fmt.Printf("reply %v\n", reply)

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
