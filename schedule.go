package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	
	// Channel which emits idle tasks which need to be scheduled on workers
	idleTasks :=  make(chan DoTaskArgs)

	// Channel which signals success of tasks
	// Exactly one output per successful task
	taskSuccess := make(chan bool)

	// Channel which emits workers which are idle
	// This is the same as the Master's registerChannel 
	idleWorkers := mr.registerChannel

	// Channel to signal the task dispatcher to quit
	quitDispatch := make(chan bool)


	// Begin scheduling idle tasks onto idle workers
	go dispatchTasks(idleTasks, idleWorkers, quitDispatch, taskSuccess)
	debug("Started task dispatcher\n")

	// Create tasks and push them as idle
	for i := 0; i < ntasks; i++ {
		var taskFile string
		if phase == mapPhase {
			taskFile = mr.files[i]
		}
		taskArgs := DoTaskArgs {
			JobName: mr.jobName,
			File: taskFile,
			Phase: phase,
			TaskNumber: i,
			NumOtherPhase: nios,
		}

		debug("Created Task %d\n", i)

		idleTasks <- taskArgs

		debug("Pushed Task %d\n", i)
	}
	debug("Created and pushed all tasks\n")

	// Wait for all tasks to complete
	for i := 0; i < ntasks; i++ {
		<- taskSuccess
	}

	// Cleanup goroutines
	// IMPORTANT: order is important
	quitDispatch <- true


	fmt.Printf("Schedule: %v phase done\n", phase)
}

// Handle new worker registrations
// Simply push them as idle
// func handleReg(registerChannel chan string, idleWorkers chan string, quitReg chan bool) {
// 	for {
// 		select {
// 		case worker := <- registerChannel:
// 			idleWorkers <- worker
// 		case <- quitReg:
// 			return
// 		}
// 	}
// }

// Dispatch idle tasks to idle workers
func dispatchTasks(idleTasks chan DoTaskArgs, idleWorkers chan string, 
	quitDispatch chan bool, taskSuccess chan bool) {
	// Queues of idle tasks and workers
	taskQueue := make([]DoTaskArgs, 0)
	workerQueue := make([]string, 0)
	for {
		select {
		case task := <- idleTasks:
			debug("Dispatcher Recvd idle task\n")
			taskQueue = append(taskQueue, task)
		case worker := <- idleWorkers:
			debug("Dispatcher Recvd idle worker\n")
			workerQueue = append(workerQueue, worker)
		case <- quitDispatch:
			// All workers are pushed back to idleWorkers
			// This is done so that future invocations of schedule
			// can make use of the workers
			go func() {
				for i := 0; i < len(workerQueue); i++ {
					idleWorkers <- workerQueue[i]
				}
			}()
			return
		}

		// Pair tasks with worker until either of the queues becomes empty
		for len(taskQueue) > 0 && len(workerQueue) > 0 {
			go doTask(taskQueue[0], workerQueue[0], idleTasks, idleWorkers, 
				taskSuccess)
			taskQueue = taskQueue[1:]
			workerQueue = workerQueue[1:]
		}
	}
}

// Execute a task on a worker
// DoTask RPC is called on the worker
func doTask(task DoTaskArgs, worker string, idleTasks chan DoTaskArgs, 
	idleWorkers chan string, taskSuccess chan bool) {

	ok := call(worker, "Worker.DoTask", &task, new(struct{}))
	// It's IMPORTANT to do this before pushing true to taskSuccess
	// so that all workers are retruned to the task dispatcher before 
	// the scheduler's wait ends
	idleWorkers <- worker
	if ! ok {
		idleTasks <- task
	} else {
		taskSuccess <- true
	}
}
