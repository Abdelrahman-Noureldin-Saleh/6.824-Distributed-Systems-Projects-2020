package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	reply := CallMaster(nil)

Loop:
	for {
		var files []string
		task := reply.Task
		switch task.TaskType {
		case mapTask:
			content := readFile(task.Input[0])
			kva := mapf(task.Input[0], string(content))
			files = writeIntermediate(kva, task.TaskId, reply.NReduce)

		case reduceTask:
			var intermediate map[string][]string
			intermediate = make(map[string][]string)
			for _, name := range task.Input {
				if len(name) == 0 {
					continue
				}
				content := readFile(name)
				pairs := strings.Split(strings.Trim(string(content), "\n"), "\n")
				for _, line := range pairs {
					pair := strings.Split(line, " ")
					x := KeyValue{Key: pair[0], Value: pair[1]}
					intermediate[x.Key] = append(intermediate[x.Key], x.Value)
				}
			}
			var final []KeyValue
			for key, val := range intermediate {
				final = append(final, KeyValue{Key: key, Value: reducef(key, val)})
			}
			name := writeFinal(final, task.TaskId)
			files = append(files, name)

		case thanks:
			break Loop

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

func writeFinal(final []KeyValue, taskNum int) string {

	sort.Sort(ByKey(final))
	filename := createFinalFileName(taskNum)
	tempFileName := fmt.Sprintf("%s-%d-red", filename, os.Getpid())
	file, err := os.OpenFile(tempFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	for _, keyValue := range final {
		if _, err := file.WriteString(fmt.Sprintf("%v %v\n", keyValue.Key, keyValue.Value)); err != nil {
			log.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(tempFileName, filename); err != nil {
		log.Fatal(err)
	}
	return filename
}

// helper function to read a file safely
func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("cannot close %v", filename)
	}
	return content
}

// writes intermediate data to Files
func writeIntermediate(intermediate []KeyValue, mapTaskNum int, nReduce int) []string {
	//fmt.Printf("task %d writing %d pairs: %v\n", mapTaskNum, len(intermediate), intermediate[0:int(math.Min(float64(len(intermediate)), 10))])
	groups := make(map[int][]KeyValue)
	for _, item := range intermediate {
		hash := ihash(item.Key) % nReduce
		groups[hash] = append(groups[hash], item)
	}
	filesNames := make([]string, nReduce)
	for key, val := range groups {
		filename := createIntermediateFileName(mapTaskNum, key)
		//
		tempFileName := fmt.Sprintf("%s-%d-map", filename, os.Getpid())
		finalFileName := fmt.Sprintf("%s", filename)
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
		filesNames[key] = filename
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
		WorkerId: os.Getpid(),
		Files:    files,
	}

	// send the RPC request, wait for the reply.
	if !call("Master.GetTask", &args, &reply) {
		os.Exit(1)
	}

	//fmt.Printf("worker %d assigned:", args.WorkerId)
	//printTask(reply.Task)
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
