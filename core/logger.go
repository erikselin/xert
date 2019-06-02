package core

import (
	"fmt"
	"log"
)

// Logger ...
type Logger interface {
	Err(msg string)
	Log(msg string)
	Logf(format string, v ...interface{})
}

type logger struct {
	workerID int
}

func (l logger) Err(msg string) error {
	return fmt.Errorf("error in worker.%d: %s", l.workerID, msg)
}

func (l logger) Log(msg string) {
	log.Printf("  [worker.%d] %s", l.workerID, msg)
}

func (l logger) Logf(format string, v ...interface{}) {
	l.log(fmt.Sprintf(format, v...))
}
