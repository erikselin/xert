package core

import (
	"fmt"
	"log"
)

type context struct {
	workerID int
	mappers  int
	reducers int
	scratch  string
	done     chan struct{}
}

func (c *context) Err(msg string) error {
	return fmt.Errorf("error in worker.%d: %s", c.workerID, msg)
}

func (c *context) Log(msg string) {
	log.Printf("  [worker.%d] %s", c.workerID, msg)
}

func (c *context) Logf(format string, v ...interface{}) {
	c.Log(fmt.Sprintf(format, v...))
}

func (c *context) Done() <-chan struct{} {
	return c.done
}

func (c *context) cancel() {
	close(c.done)
}
