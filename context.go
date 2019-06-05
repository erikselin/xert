package xrt

import (
	"errors"
	"io"
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

	// Done implements the WithCancel context pattern and can be used by
	// workers to determine if they should abandon any current work. The chan
	// returned by Done will be closed if any error has occured in other
	// workers and the Job has failed.
	Done() <-chan struct{}
}

type context struct{}

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

func (c *context) Done() <-chan struct{} {
	return make(chan struct{})
}

func (c *context) withInputReader(func(io.Reader) error) error {
	return nil
}

func (c *context) withOutputWriter(func(io.Writer) error) error {
	return nil
}

func (c *context) withRecordReader(func(RecordReader) error) error {
	return nil
}

func (c *context) withRecordWriter(func(RecordWriter) error) error {
	return nil
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
