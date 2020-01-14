package mapreduce

import (
	"fmt"
	"log"
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

	for len(tasksList) > 0 {
		// use `queue` to save task list
		taskNum := tasksList[0]
		tasksList = append(tasksList[:0], tasksList[1:]...)

		// use `sync.WaitGroup` to control goroutine
		// make master goroutine wait until all the other goroutines have finished tasks
		wg.Add(1)

		// `registerChan` is a blocked channel
		// take out an worker address, it will block there
		workerAddress := <-registerChan

		go func() {
			// use the `call()` function in `mapreduce/common_rpc.go` to send an RPC to a worker.
			response := call(workerAddress, "Worker.DoTask", DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[taskNum],
				Phase:         phase,
				TaskNumber:    taskNum,
				NumOtherPhase: nother,
			}, nil)

			wg.Done()

			if response {
				// if call() returns `true`, the server responded
				// this worker can be reused
				registerChan <- workerAddress
				log.Printf("schedule call RPC \"Worker.DoTask\" succeeded! worker address = %s \n, task = %d \n", workerAddress, taskNum)
			} else {
				// if call() returns `false`, the server may be time out or the worker is unusable
				// the task should be put back on the tasksList
				tasksList = append(tasksList, taskNum)
				log.Printf("schedule call RPC \"Worker.DoTask\" error! worker address = %s \n, task = %d \n", workerAddress, taskNum)
			}
		}()
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
