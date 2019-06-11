package xrt

import (
	"errors"
	"log"
)

// Context ...
type Context interface {

	// WorkerID returns a unique number in the [0,mappers) range for mapper
	// workers and a unique number in the [0,reducers) range for reducer
	// workers.
	WorkerID() int

	// The number of mapper workers in this xrt job.
	Mappers() int

	// The number of reducer workers in this xrt job.
	Reducers() int

	// Scratch returns a temporary directory that will be automatically removed
	// after the xrt job terminates. It can be used to store any temporary data
	// that should be deleted once the job is done. The scratch directory is
	// scoped to the xrt job, not the individual worker. Thus, any writes by
	// multiple workers needs to be coordinated (generally this is done by
	// naming files using the WorkerID).
	Scratch() string

	// Err return a error with message scoped to the worker.
	Err(string) error

	// Log logs a message to the xrt log scoped to the worker.
	Log(string)

	// Logf logs a formatted message to the xrt log scoped to the worker.
	Logf(string, ...interface{})
}

type context struct {
}

func (c *context) WorkerID() int {
	return 0
}

func (c *context) Mappers() int {
	return 1
}

func (c *context) Reducers() int {
	return 1
}

func (c *context) Scratch() string {
	return ""
}

func (c *context) Err(msg string) error {
	return errors.New(msg)
}

func (c *context) Log(msg string) {
	log.Print(msg)
}

func (c *context) Logf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

//import (
//	"fmt"
//	"log"
//)
//
//type context struct {
//	workerID int
//	mappers  int
//	reducers int
//	scratch  string
//	done     chan struct{}
//}
//
//func (c *context) Err(msg string) error {
//	return fmt.Errorf("error in worker.%d: %s", c.workerID, msg)
//}
//
//func (c *context) Log(msg string) {
//	log.Printf("  [worker.%d] %s", c.workerID, msg)
//}
//
//func (c *context) Logf(format string, v ...interface{}) {
//	c.Log(fmt.Sprintf(format, v...))
//}
//
//func (c *context) Done() <-chan struct{} {
//	return c.done
//}
//
//func (c *context) cancel() {
//	close(c.done)
//}
