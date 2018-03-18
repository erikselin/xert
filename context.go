package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var (
	mu      sync.Mutex
	stopped = false
	procs   = make(map[int]*os.Process)
)

type context struct {
	workerID int
	mappers  int
	reducers int
}

func (c context) err(msg string) error {
	return fmt.Errorf("error in worker.%d: %s", c.workerID, msg)
}

func (c context) log(msg string) {
	log.Printf("  [worker.%d] %s", c.workerID, msg)
}

func (c context) logf(format string, v ...interface{}) {
	c.log(fmt.Sprintf(format, v...))
}

func (c context) exec(
	command string,
	stdinHandler func(context, io.WriteCloser) error,
	stdoutHandler func(context, io.ReadCloser) error,
	stderrHandler func(context, io.ReadCloser) error,
) error {
	args := strings.Fields(command)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("WORKER_ID=%d", c.workerID),
		fmt.Sprintf("MAPPERS=%d", c.mappers),
		fmt.Sprintf("REDUCERS=%d", c.reducers),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	errc := make(chan error)
	go func() { errc <- stdinHandler(c, stdin) }()
	go func() { errc <- stdoutHandler(c, stdout) }()
	go func() { errc <- stderrHandler(c, stderr) }()
	if err := start(cmd); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}
	return wait(cmd)
}

func start(c *exec.Cmd) error {
	mu.Lock()
	defer mu.Unlock()
	if stopped {
		return errors.New("no new processes may be started")
	}
	if err := c.Start(); err != nil {
		return err
	}
	procs[c.Process.Pid] = c.Process
	return nil
}

func wait(c *exec.Cmd) error {
	err := c.Wait()
	mu.Lock()
	delete(procs, c.Process.Pid)
	mu.Unlock()
	return err
}

func killAll() {
	mu.Lock()
	defer mu.Unlock()
	stopped = true
	for _, p := range procs {
		p.Kill()
	}
}
