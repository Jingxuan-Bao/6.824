package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Job struct {
	index     int
	filename  string
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	jobUnfinished   []Job
	jobWorking      []Job
	nReduce         int
	currentJobStage string
}

var mutex = sync.RWMutex{}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Return a job to the worker
func (c *Coordinator) GetAJob(args *ApplyJobArgs, reply *ApplyJobReply) error {
	mutex.Lock()

	if len(c.jobUnfinished) == 0 {
		reply.JobType = "wait"
	} else {

		job := c.jobUnfinished[0]
		job.startTime = time.Now()
		c.jobWorking = append(c.jobWorking, job)

		c.jobUnfinished = c.jobUnfinished[1:]

		// reply info
		reply.Filename = job.filename
		reply.Index = job.index
		reply.JobType = c.currentJobStage
		reply.NReduce = c.nReduce
	}

	mutex.Unlock()

	return nil
}

// Job finish
func (c *Coordinator) FinishAJob(args *JobFinishArgs, reply *JobFinishReply) error {
	if c.currentJobStage != args.JobType {
		reply.Status = "Job type inconsistent."
	}
	mutex.Lock()

	matchIdx := -1
	for idx, job := range c.jobWorking {
		if job.index == args.Index {
			matchIdx = idx
			break
		}
	}
	if matchIdx != -1 {
		fmt.Printf("Finish %s Job. Index %d. SpendTime %fs \n", args.JobType, args.Index, time.Since(c.jobWorking[matchIdx].startTime).Seconds())
		c.jobWorking = append(c.jobWorking[:matchIdx], c.jobWorking[matchIdx+1:]...)
		reply.Status = fmt.Sprintf("Success.")
	} else {
		reply.Status = fmt.Sprintf("Cannot find the job. idx: %d", args.Index)
	}

	mutex.Unlock()
	return nil
}

func (c *Coordinator) check_thread() {
	for {
		mutex.Lock()

		// check if job is timeout
		timeoutJob := []Job{}
		notTimeoutJob := []Job{}
		for _, job := range c.jobWorking {
			if time.Since(job.startTime).Seconds() > 10.0 {
				// timeout
				timeoutJob = append(timeoutJob, job)
			} else {
				notTimeoutJob = append(notTimeoutJob, job)
			}
		}

		c.jobUnfinished = append(c.jobUnfinished, timeoutJob...)
		c.jobWorking = notTimeoutJob

		// check if map proceduce finish
		if len(c.jobUnfinished) == 0 && len(c.jobWorking) == 0 {
			if c.currentJobStage == "map" {
				c.currentJobStage = "reduce"

				for i := 0; i < c.nReduce; i++ {
					job := Job{
						index:     i,
						filename:  fmt.Sprintf("mr-*-%d", i),
						startTime: time.Now(),
					}
					c.jobUnfinished = append(c.jobUnfinished, job)
				}
			} else {
				print("All map and reduce jobs are finished.\n")
				mutex.Unlock()
				return
			}
		}
		mutex.Unlock()
		time.Sleep(time.Second)
	}
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
	// new a thread
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mutex.RLock()
	ret = len(c.jobUnfinished) == 0 && len(c.jobWorking) == 0 && c.currentJobStage == "reduce"
	mutex.RUnlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.currentJobStage = "map"

	for i := 0; i < len(files); i++ {
		job := Job{i, files[i], time.Now()}
		c.jobUnfinished = append(c.jobUnfinished, job)
	}

	// thread to check the timeout or all job finish
	go c.check_thread()

	c.server()
	return &c
}
