package mapreduce

import (
	"fmt"
	"net"
)

// Master holds all the state that the master needs to keep track of. Of
// particular importance is idleWorker, the channel that notifies the master of
// workers that have gone idle and are in need of new work.
type Master struct {
	address         string
	registerChannel chan string
	doneChannel     chan bool
	workers         []string

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	alive bool
	l     net.Listener
	stats []int
}

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive jobs.
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	mr.registerChannel <- args.Worker
	return nil
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.alive = true
	mr.registerChannel = make(chan string)
	mr.doneChannel = make(chan bool)
	return
}

// Sequential runs map and reduce job sequentially, waiting for each job to
// complete before scheduling the next.
func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

// Distributed schedules map and reduce jobs on workers that register with the
// master over RPC.
func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce, mr.schedule, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each job on workers as they become available. Each map job bins
// its output in a number of bins equal to the given number of reduce jobs.
// Once all the mappers have finished, workers are assigned reduce jobs.
//
// When all jobs have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all jobs have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of jobs each work has performed.
func (mr *Master) killWorkers() []int {
	njobs := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			njobs = append(njobs, reply.Njobs)
		}
	}
	return njobs
}