package mr

import (
	"encoding/json"
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

	// uncomment to send the Example RPC to the coordinator.
	for {
		job := CallToGetAJob()
		switch job.JobType {
		case "wait":
			fmt.Println("Please wait, not runnable job in coordinator at the moment, will Retry later...")
			break
		case "map":
			handleMapJob(job.Filename, job.Index, job.NReduce, mapf)
			CallToFinishAJob(job)
			break
		case "reduce":
			handleReduceJob(job.Filename, job.Index, job.NReduce, reducef)
			CallToFinishAJob(job)
			break
		}

		time.Sleep(time.Second)
	}

}

func handleMapJob(filename string, mapIdx int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	hashList := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		file_idx := ihash(kv.Key) % nReduce
		hashList[file_idx] = append(hashList[file_idx], kv)
	}
	for reduceIdx, objList := range hashList {
		file_name := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
		tmp_file_name := fmt.Sprintf("%s-%d", filename, time.Now().Unix())
		tmp_file, _ := os.Create(tmp_file_name)
		enc := json.NewEncoder(tmp_file)
		for _, kv := range objList {
			enc.Encode(&kv)
		}
		os.Rename(tmp_file_name, file_name)
	}
}

func handleReduceJob(filename string, idx int, nReduce int, reducef func(string, []string) string) {
	// read all file map mr-*-idx
	reduceList := []string{}
	for _, name := range readCurrentDir() {
		if len(name) >= 6 &&
			name[3:6] != "out" &&
			name[:3] == "mr-" &&
			name[len(name)-1-len(strconv.Itoa(idx)):] == fmt.Sprintf("-%d", idx) {
			reduceList = append(reduceList, name)
		}
	}

	// read all KeyValue into one List
	mergeKV := []KeyValue{}
	for _, filename := range reduceList {
		file, err := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mergeKV = append(mergeKV, kv)
		}
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}

	sort.Sort(ByKey(mergeKV))

	outputName := fmt.Sprintf("mr-out-%d", idx)
	tmpOutName := fmt.Sprintf("%s-%d", outputName, time.Now().Unix())

	ofile, _ := os.Create(tmpOutName)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(mergeKV) {
		j := i + 1
		// check until where the key is the same !
		for j < len(mergeKV) && mergeKV[j].Key == mergeKV[i].Key {
			j++
		}
		values := []string{}
		// add all these value with the same key to a same array.
		for k := i; k < j; k++ {
			values = append(values, mergeKV[k].Value)
		}
		// call reduce function
		output := reducef(mergeKV[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", mergeKV[i].Key, output)

		// update i to skip this key!
		i = j

		// continue with next Key.
	}

	ofile.Close()
	os.Rename(tmpOutName, outputName)
}
func CallToFinishAJob(job ApplyJobReply) JobFinishReply {
	args := JobFinishArgs{
		Index:   job.Index,
		JobType: job.JobType,
	}
	reply := JobFinishReply{}
	ok := call("Coordinator.FinishAJob", &args, &reply)
	if ok {
		fmt.Printf("Success to finish a job. Reply %s.\n", reply.Status)
	} else {
		fmt.Printf("Worker try to finish a job fail. Process end. \n")
		os.Exit(0)
	}

	return reply
}

func CallToGetAJob() ApplyJobReply {
	args := ApplyJobArgs{}
	reply := ApplyJobReply{}
	ok := call("Coordinator.GetAJob", &args, &reply)
	if ok {
		fmt.Println("Success to get a job", reply.Index, reply.Filename, reply.JobType)
	} else {
		fmt.Printf("Worker try to get a job fail. Process end. \n")
		os.Exit(0)
	}

	return reply
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

// Return all file names in current DIR
func readCurrentDir() []string {
	file, err := os.Open(".")
	if err != nil {
		log.Fatalf("failed opening directory: %s", err)
	}
	defer file.Close()

	list, _ := file.Readdirnames(0) // 0 to read all files and folders
	return list
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
