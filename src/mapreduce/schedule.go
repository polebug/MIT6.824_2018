package mapreduce

import (
	"fmt"
	"sync"
)

// schedule() hands out tasks to the available workers by sending a `Worker.DoTask` RPC to the worker,
// and waits until all tasks have completed, and then return.
func schedule(
	jobName string,
	mapFiles []string, // the names of the files that are the inputs to the map phase, one per map task
	nReduce int, // the number of reduce tasks
	phase jobPhase, // mapPhase or reducePhase
	registerChan chan string, // registerChan yields a stream of registered workers
) {
	var (
		ntasks    int   // the number of map tasks
		nother    int   // number of inputs (for reduce) or outputs (for map)
		tasksList []int // todo tasks list
		wg        sync.WaitGroup
	)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nother = nReduce
	case reducePhase:
		ntasks = nReduce
		nother = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nother)

	// initialize task list
	for i := 0; i < ntasks; i++ {
		tasksList = append(tasksList, i)
	}

	for _, taskNum := range tasksList {
		// schedule() must give each worker a sequence of tasks, one at a time
		// use `sync.WaitGroup` to control goroutine
		wg.Add(1)

		go func(taskNum int) {
			// use the `call()` function in `mapreduce/common_rpc.go` to send an RPC to a worker.
			workerAddress := <-registerChan
			doTaskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[taskNum],
				Phase:         phase,
				TaskNumber:    taskNum,
				NumOtherPhase: nother,
			}
			response := call(workerAddress, "Worker.DoTask", doTaskArgs, nil)
			if response {
				// if call() returns `true`, the server responded
				// this worker can be reused
				wg.Done()
				registerChan <- workerAddress
			} else {
				// if call() returns `false`, the server may be time out
				// the task should be put back on the tasksList
				tasksList = append(tasksList, taskNum)
				wg.Done()
			}
		}(taskNum)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
