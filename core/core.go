// Package core ...
package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"time"
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

// RecordReader ...
type RecordReader interface {

	// Next ...
	Next() bool

	// Record ...
	Record() []byte

	// Err ...
	Err() error
}

// RecordWriter ...
type RecordWriter interface {

	// Write ...
	Write(int, []byte) error
}

// Config ...
type Config struct {
	Mappers  int
	Reducers int
	TempDir  string
	Memory   string
}

// RunM ...
func RunM(
	mapper func(Context) error,
	config Config,
) error {
	return run(
		"",
		func(c Context, r io.Reader, w io.Writer, _ RecordWriter) error {
			return mapper(c)
		},
		"",
		output,
		config,
	)
}

// RunIM ...
func RunIM(
	input string,
	mapper func(Context, io.Reader) error,
	config Config,
) error {
	return run(
		input,
		func(c Context, r io.Reader, w io.Writer, _ RecordWriter) error {
			return mapper(c, r)
		},
		"",
		output,
		config,
	)
}

func RunMO(
	mapper func(Context, io.Writer) error,
	output string,
	config Config,
) error {
	return run(
		"",
		func(c Context, r io.Reader, w io.Writer, _ RecordWriter) error {
			return mapper(c, w)
		},
		output,
		output,
		config,
	)
}

func RunIMO(
	input string,
	mapper func(Context, io.Reader, io.Writer) error,
	output string,
	config Config,
) error {
	return run(
		input,
		func(c Context, r io.Reader, w io.Writer, _ RecordWriter) error {
			return mapper(c, r, w)
		},
		nil,
		output,
		config,
	)
}

func RunMR(
	mapper func(Context, RecordWriter) error,
	reducer func(Context, RecordReader) error,
	config Config,
) error {
	return run(
		"",
		func(c Context, r io.Reader, _ io.Writer, w RecordWriter) error {
			return mapper(c, w)
		},
		func(c Context, r RecordReader, w io.Writer) error {
			return reducer(c, r)
		},
		"",
		config,
	)
}

func RunIMR(
	input string,
	mapper func(Context, io.Reader, RecordWriter) error,
	reducer func(Context, RecordReader) error,
	config Config,
) error {
	return run(
		input,
		func(c Context, r io.Reader, _ io.Writer, w RecordWriter) error {
			return mapper(c, r, w)
		},
		func(c Context, r RecordReader, w io.Writer) error {
			return reducer(c, r)
		},
		"",
		config,
	)
}

func RunMRO(
	mapper func(Context, RecordWriter) error,
	reducer func(Context, RecordReader, io.Writer) error,
	output string,
	config Config,
) error {
	return run(
		"",
		func(c Context, r io.Reader, _ io.Writer, w RecordWriter) error {
			return mapper(c, w)
		},
		func(c Context, r RecordReader, w io.Writer) error {
			return reducer(c, r, w)
		},
		output,
		config,
	)
}

func RunIMRO(
	input string,
	mapper func(Context, io.Reader, RecordWriter) error,
	reducer func(Context, RecordReader, io.Writer) error,
	output string,
	config Config,
) error {
	return run(
		input,
		func(c Context, r io.Reader, _ io.Writer, w RecordWriter) error {
			return mapper(c, r, w)
		},
		func(c Context, r RecordReader, w io.Writer) error {
			return reducer(c, r, w)
		},
		output,
		config,
	)
}

func run(
	input string,
	mapper func(Context, io.Reader, io.Writer, RecordWriter) error,
	reducer func(Context, RecordReader, io.Writer) error,
	output string,
	config Config,
) error {
	startTime := time.Now()
	conf, err := newConfig(config)
	if err != nil {
		return err
	}
	log.Print("starting xrt job")
	log.Print("")
	logConfig(conf)
	log.Print("")
	log.Print("running mapper stage")
	log.Print("")
	startTimeMappers := time.Now()
	buffers, err := runMappers(conf, mapper)
	if err != nil {
		rollback(once, exit, err)
	}
	durationMappers := time.Since(startTimeMappers)
	l.Print("")
	//var durationReducers time.Duration
	//if hasReducer() {
	//	l.Print("running reducer stage")
	//	l.Print("")
	//	startTimeReducers := time.Now()
	//	if err := runReducers(conf, reducers, buffers); err != nil {
	//		rollback(once, exit, err)
	//	}
	//	durationReducers = time.Since(startTimeReducers)
	//	l.Print("")
	//}
	//if hasOutput() {
	//	l.Print("committing")
	//	l.Print("")
	//	commit()
	//}
	//l.Printf("  mappers runtime: %s", durationMappers.String())
	//if hasReducer() {
	//	l.Printf("  reducers runtime: %s", durationReducers.String())
	//}
	//l.Printf("  total runtime: %s", time.Since(startTime).String())
	//l.Print("")
	//l.Print("success")
	//if !hasOutput() {
	//	printOutput()
	//}
	//cleanup()
}

func logConfig(conf *config) {
	log.Print("configuration:")
	log.Print("")
	if conf.hasInput() {
		log.Printf("  input:    %s", conf.input)
	} else {
		l.Print("  input:    (none)")
	}
	l.Printf("  mapper:   %s", conf.mapper)
	l.Printf("  mappers:  %d", conf.mappers)
	if conf.hasReducer() {
		log.Printf("  reducer:  %s", conf.reducer)
		log.Printf("  reducers: %d", conf.reducers)
	} else {
		log.Print("  reducer:  (none)")
		log.Print("  reducers: (none)")
	}
	if conf.hasOutput() {
		log.Printf("  output:   %s", conf.output)
	} else {
		log.Print("  output:   (none)")
	}
	log.Printf("  memory:   %s", conf.memoryString)
	log.Printf("  tempdir:  %s", conf.tempDir)
}

// startInterruptHandler launches a handler that will catch the first interrupt signal and attempt
// a graceful termination (mainly to deal with ctrl-c)
func startInterruptHandler(once sync.Once, exit ExitFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		rollback(once, exit, errors.New("received interrupt - aborting job"))
	}()
}

//func runMany(workers int, worker func(context) error) error {
//	errc := make(chan error)
//	for i := 0; i < workers; i++ {
//		go func(wid int) { errc <- worker(context{wid, mappers, reducers}) }(i)
//	}
//	for i := 0; i < workers; i++ {
//		if err := <-errc; err != nil {
//			return err
//		}
//	}
//	return nil
//}
func runMappers(conf *config, mapper MapperFunc) ([][]*buffer, error) {
	in := newInput(conf)
	out := new

	var buffers [][]*buffer
	buffers = make([][]*buffer, conf.mappers)
	for i := range buffers {
		buffers[i] = make([]*buffer, conf.reducers)
	}

	if conf.hasOutput() || conf.hasReducer() {
		if tempDir, err = ioutil.TempDir(conf.tempDir, "xrt-"); err != nil {
			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempDir, err)
		}
	}
	if conf.hasOutput() {
		if err = os.Mkdir(tempOutput, 0700); err != nil {
			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempOutput, err)
		}
	}
	if conf.hasReducer() {
		if err = os.Mkdir(tempSpill, 0700); err != nil {
			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempSpill, err)
		}
	}

	input := newInput(conf)
	//	errc := make(chan error)
	//	for i := 0; i < mappers; i++ {
	//		go func(wid int) {
	//			l := logger{wid}
	//			errc <- worker(MapperContext{wid, mapper, mappers, reducers})
	//		}(i)
	//	}
	//	for i := 0; i < workers; i++ {
	//		if err := <-errc; err != nil {
	//			return err
	//		}
	//	}
	//	return nil
}

func mapWorker(c context) error {
	c.log("mapper starting")
	defer c.log("done")
	if hasReducer() {
		bufMem := memory / (mappers * reducers)
		for i := range buffers[c.workerID] {
			spillDir := path.Join(tempSpill, strconv.Itoa(c.workerID), strconv.Itoa(i))
			buffers[c.workerID][i] = newBuffer(bufMem, spillDir)
		}
	}
	if err := c.exec(mapper, mapStdinHandler, mapStdoutHandler, logStream); err != nil {
		return err
	}
	if hasReducer() && reducers <= mappers {
		c.log("sorting")
		for i, b := range buffers[c.workerID] {
			b.sort()
			if b.needExternalSort() {
				c.logf("merging %d spill files for reducer %d", b.spills, i)
				if err := b.externalSort(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func mapStdinHandler(c context, w io.WriteCloser) error {
	if hasInput() {
		return inputStream(c, w, inputs)
	}
	return w.Close()
}

func mapStdoutHandler(c context, r io.ReadCloser) error {
	if hasReducer() {
		return intermediateMapStream(c, r, buffers[c.workerID])
	}
	return outputStream(c, r, tempOutput)
}

func reduceWorker(c context) error {
	c.log("reducer starting")
	defer c.log("done")
	if hasReducer() && reducers > mappers {
		c.log("sorting")
		for i, b := range buffers[c.workerID] {
			b.sort()
			if b.needExternalSort() {
				c.logf("merging %d spill files for reducer %d", b.spills, i)
				if err := b.externalSort(); err != nil {
					return err
				}
			}
		}
	}
	records := 0
	for i := range buffers {
		records += buffers[i][c.workerID].records
	}
	c.logf("processing %d records", records)
	return c.exec(reducer, reduceStdinHandler, reduceStdoutHandler, logStream)
}

func reduceStdinHandler(c context, w io.WriteCloser) error {
	bufs := make([]*buffer, len(buffers))
	for i := range buffers {
		bufs[i] = buffers[i][c.workerID]
	}
	return intermediateReduceStream(c, w, bufs)
}

func reduceStdoutHandler(c context, r io.ReadCloser) error {
	return outputStream(c, r, tempOutput)
}

// rollback ensure graceful termination of a failed job. It first executes the shutdownFunc
// before cleaning up any temporary data stored on disk.
func rollback(once sync.Once, exit ExitFunc, err error) {
	once.Do(func() {
		log.Print("error - attempting rollback")
		log.Print("")
		log.Print(err)
		if exit != nil {
			exit()
		}
		cleanup()
		log.Print("failed")
		os.Exit(1)
	})
}

// commit ensures transactional termination of succesfull jobs. If the job is configured to create
// output commit uses a directory move to transactioanlly "commit" the output from a temporary
// folder to the final output folder.
func commit() {
	if err := os.Rename(tempOutput, output); err != nil {
		log.Printf("  error moving output data from %s to %s - %v", tempOutput, output, err)
		log.Printf("  temporary data directory %s was not removed", tempDir)
		log.Print("failed")
		os.Exit(1)
	}
}

// printOutput reads the output from the temporary directory and copies it to stdout. This is used
// by runs without output to display the output in the terminal instead of writting it to a file.
func printOutput() {
	files, err := ioutil.ReadDir(tempOutput)
	if err != nil {
		log.Printf("  error reading output data in %s - %v", tempOutput, err)
		os.Exit(1)
	}
	for _, file := range files {
		filename := path.Join(tempOutput, file.Name())
		f, err := os.Open(filename)
		if err != nil {
			log.Printf("  error reading output data in %s - %v", filename, err)
			os.Exit(1)
		}
		r := bufio.NewReader(f)
		w := bufio.NewWriter(os.Stdout)
		if _, err := io.Copy(w, r); err != nil {
			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
		}
		if err := w.Flush(); err != nil {
			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
		}
	}
}

// cleanup removes any remaining temporary files.
func cleanup() {
	// BUG this will break on windows since it does not allow removal of open files and by the
	// time this is called it is possible fds in the tempdir are still open.
	if err := os.RemoveAll(tempDir); err != nil {
		log.Printf("  failed to remove temporary data directory %s - %v", tempDir, err)
	}
}

func hasInput() bool {
	return len(input) > 0
}

func hasMapper() bool {
	return len(mapper) > 0
}

func hasReducer() bool {
	return len(reducer) > 0
}

func hasOutput() bool {
	return len(output) > 0
}
