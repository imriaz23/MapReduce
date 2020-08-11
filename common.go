package MapReduce

import (
	"fmt"
	"strconv"
)
const debugEnabled = false

//print only if debugEnabled const has been set to true
func debug(format string, a ...interface{})(n int,err error){
	if debugEnabled{
		n,err = fmt.Printf(format, a...)
	}
	return
}

// Job phase will indicate whether a task is scheduled as a map or reduce task

type jobPhase string

const(
	mapPhase	jobPhase = "Map"
	reducePhase 		 ="Reduce"
)

type KeyValue struct {
	Key string
	value string
}

func reduceName(jobName string ,mapTask int,reduceTask int) string{
	return "mrtmp." + jobName + "_" + strconv.Itoa(mapTask) + "_" + strconv.Itoa(reduceTask)
}
func mergeName(jobName string ,reduceTask int) string{
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}