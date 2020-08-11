package MapReduce

import (
	"fmt"
	"net/rpc"
)

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will break.

// DoTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type DoTaskArgs struct {
	JobName    string
	File        string    // the file to process the file to be processed
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase task序号

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int
	// reduce input number or map output number
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type  RegisterArgs  struct {
	Worker string
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if  errx  !=  nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}