package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func handleRegister(mr *MapReduce) {
	for worker := range mr.registerChannel {
		mr.Workers[worker] = &WorkerInfo{worker}
		mr.availableWorker <- worker
	}
	fmt.Printf("RunMaster: Register OK\n")
}

func (mr *MapReduce) RunMaster() *list.List {

	go handleRegister(mr)

	var assignJobs = func(nNumber int, numOtherPhase int, operation JobType) {
		jobChannel := make(chan int)
		go func(jobChannel chan int) {
			for i := 0; i < nNumber; i++ {
				jobChannel <- i
			}
		}(jobChannel)
		doneJob := make(chan int)
		go func() {
			for {
				jobNumber, more := <-jobChannel
				if !more {
					break
				}
				worker := <-mr.availableWorker
				args := &DoJobArgs{
					JobNumber:     jobNumber,
					File:          mr.file,
					Operation:     operation,
					NumOtherPhase: numOtherPhase}
				var reply DoJobReply
				go func() {
					ok := call(mr.Workers[worker].address, "Worker.DoJob", args, &reply)
					if ok == false {
						fmt.Printf("DoWork: RPC %s dojob error\n", worker)
						jobChannel <- jobNumber
					} else {
						doneJob <- jobNumber
					}
					mr.availableWorker <- worker
				}()
			}
		}()
		jobCount := 0
		for {
			<-doneJob
			jobCount++
			fmt.Println(jobCount)
			if jobCount >= nNumber {
				close(jobChannel)
				break
			}
		}
	}

	assignJobs(mr.nMap, mr.nReduce, Map)
	assignJobs(mr.nReduce, mr.nMap, Reduce)
	return mr.KillWorkers()
}
