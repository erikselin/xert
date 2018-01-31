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
	mu sync.Mutex

	// stopped indicates wether no more processes can be started. When set to
	// true no more processes can be started.
	stopped = false

	// procs is the set of running processes
	procs = make(map[int]*os.Process)
)

// context contains the variables that identifies a worker.
type context struct {
	workerID int
	workers  int
}

// error returns a workerID-prefixed error message
func (ctx *context) err(msg string) error {
	return fmt.Errorf("error in worker.%d: %s", ctx.workerID, msg)
}

// log logs a workerID-prefixed message
func (ctx *context) log(msg string) {
	log.Printf("  [worker.%d] %s", ctx.workerID, msg)
}

// logf formats and logs a workerID-prefixed message
func (ctx *context) logf(format string, v ...interface{}) {
	ctx.log(fmt.Sprintf(format, v...))
}

// execCmd starts a processes within the context. The new processes will run the
// command cmd and the standard streams will be processed by the provided stdin,
// stdout and stderr handlers. context scoped environment variables will be set
// for the processes. exec blocks until the processes terminates or until the
// first error (either from the processes terminating with a non-zero exit code
// or if any of the stream handlers returns an error).
func (ctx *context) exec(
	cmd string,
	handleStdin func(*context, io.WriteCloser) error,
	handleStdout func(*context, io.ReadCloser) error,
	handleStderr func(*context, io.ReadCloser) error,
) error {
	args := strings.Fields(cmd)

	c := exec.Command(args[0], args[1:]...)
	c.Env = append(
		os.Environ(),
		fmt.Sprintf("WORKER_ID=%d", ctx.workerID),
		fmt.Sprintf("WORKER_COUNT=%d", ctx.workers),
	)

	stdin, err := c.StdinPipe()
	if err != nil {
		return err
	}

	stdout, err := c.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := c.StderrPipe()
	if err != nil {
		return err
	}

	errc := make(chan error)

	go func() { errc <- handleStdin(ctx, stdin) }()
	go func() { errc <- handleStdout(ctx, stdout) }()
	go func() { errc <- handleStderr(ctx, stderr) }()

	if err := startCmd(c); err != nil {
		return err
	}

	for i := 0; i < 3; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}

	return waitCmd(c)
}

// runMany spawns n=workers routines running the fn function. Each fn function is
// past a context which contains the number of workers as well as the individual
// workerID. run blocks until all spawned routines terminate or until the first
// error.
func runMany(workers int, fn func(*context) error) error {
	errc := make(chan error)

	for i := 0; i < workers; i++ {
		go func(wid int) {
			c := &context{
				workerID: wid,
				workers:  workers,
			}
			errc <- fn(c)
		}(i)
	}

	for i := 0; i < workers; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}

	return nil
}

// killAll kills all processes in the set of running processes and sets a flag
// which ensures that no new processes are launched.
func killAll() {
	mu.Lock()
	defer mu.Unlock()

	stopped = true
	for _, p := range procs {
		p.Kill()
	}
}

// start launches the command c and adds the resulting process to the set of
// running processes.
func startCmd(c *exec.Cmd) error {
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

// wait blocks until the command c terminates and removes the process from the
// set of running proceses once it is done.
func waitCmd(c *exec.Cmd) error {
	err := c.Wait()

	mu.Lock()
	delete(procs, c.Process.Pid)
	mu.Unlock()

	return err
}
