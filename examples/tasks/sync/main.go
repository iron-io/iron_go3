/*
	This code sample demonstrates how to execute a worker in synchronous manner

	http://dev.iron.io/worker/reference/api/
*/
package main

import (
	"fmt"
	"github.com/iron-io/iron_go3/worker"
)

func main() {
	task := worker.Task{CodeName: "iron/hello"}
	w := worker.New()
	result, err := w.TaskRun(task)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(result)
}
