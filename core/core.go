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

// version holds the current main xrt core version.
var version = "0.4.0"

// Config ...
type Config struct {
	Mappers      int
	Reducers     int
	TempDir      string
	Memory       string
	LoggerOutput io.Writer
}

// RunM ...
func RunM(
	mapper func(Context) error,
	config Config,
) error {
	job, err := setup(config)
	if err != nil {
		return err
	}
	return job.runMap(
		func(c Context) error {
			return mapper(c)
		},
	)
}

// RunIM ...
func RunIM(
	input string,
	mapper func(Context, io.Reader) error,
	config Config,
) error {
	job, err := newJob(config)
	if err != nil {
		return err
	}
	input, err := newInput(job, input)
	if err != nil {
		return err
	}
	return job.runMap(
		func(c Context) error {
			r := input.newInputReader(c)
			return mapper(c, r)
		},
	)
}

func RunMO(
	mapper func(Context, io.Writer) error,
	output string,
	config Config,
) error {
	job, err := newJob(config)
	if err != nil {
		return err
	}
	output, err := newOutput(job, output)
	if err != nil {
		return err
	}
	if err := job.runMap(
		func(c Context) error {
			w := output.newOutputWriter(c)
			return mapper(c, w)
		},
	); err != nil {
		return err
	}
	return output.commit()
}

func RunIMO(
	input string,
	mapper func(Context, io.Reader, io.Writer) error,
	output string,
	config Config,
) error {

}

func RunMR(
	mapper func(Context, RecordWriter) error,
	reducer func(Context, RecordReader) error,
	config Config,
) error {

}

func RunIMR(
	input string,
	mapper func(Context, io.Reader, RecordWriter) error,
	reducer func(Context, RecordReader) error,
	config Config,
) error {

}

func RunMRO(
	mapper func(Context, RecordWriter) error,
	reducer func(Context, RecordReader, io.Writer) error,
	output string,
	config Config,
) error {

}

func RunIMRO(
	input string,
	mapper func(Context, io.Reader, RecordWriter) error,
	reducer func(Context, RecordReader, io.Writer) error,
	output string,
	config Config,
) error {

}

//// Run ...
//func (j JobM) Run() error {
//	conf, err := newConfig(j.Mappers, 0, nil, "", j.TempDir, j.LoggerOutput)
//	if err != nil {
//		return err
//	}
//	return run(
//		conf,
//		func(c Context) error {
//			return j.Mapper(c)
//		},
//		nil,
//	)
//}
//
//// JobIM ...
//type JobIM struct {
//	Mappers      int
//	Mapper       func(Context, io.Reader) error
//	Input        string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobIM) Run() error {
//	conf, err := newConfig(j.Mappers, 0, nil, "", j.TempDir, j.LoggerOutput)
//	if err != nil {
//		return err
//	}
//	input, err := newInput(conf.input)
//	if err != nil {
//		return err
//	}
//	return run(
//		conf,
//		func(c Context) error {
//			r := input.newInputReader(c)
//			return j.Mapper(c, r)
//		},
//		nil,
//	)
//}
//
//// JobMO ...
//type JobMO struct {
//	Mappers      int
//	Mapper       func(Context, io.Writer) error
//	Output       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobMO) Run() error {
//	conf, err := newConfig(j.Mappers, 0, nil, "", j.TempDir, j.LoggerOutput)
//	if err != nil {
//		return err
//	}
//	output, err := newOutput(conf.tempDir, conf.output)
//	if err != nil {
//		return err
//	}
//	return run(
//		conf,
//		func(c Context) error {
//			w := output.newOutputWriter(c)
//			return j.Mapper(c, w)
//		},
//		nil,
//	)
//}
//
//// JobIMO ...
//type JobIMO struct {
//	Mappers      int
//	Mapper       func(Context, io.Reader, io.Writer) error
//	Input        string
//	Output       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobIMO) Run() error {
//	input, err := newInput(j.Input)
//	if err != nil {
//		return err
//	}
//	output, err := newOutput(j.TempDir, j.Output)
//	if err != nil {
//		return err
//	}
//	return run(
//		j.Mappers,
//		func(c Context) error {
//			r := input.newInputReader(c)
//			w := output.newOutputWriter(c)
//			return j.Mapper(c, r, w)
//		},
//		0,
//		nil,
//		"",
//		j.TempDir,
//		j.LoggerOutput,
//	)
//}
//
//// JobMR ...
//type JobMR struct {
//	Mappers      int
//	Mapper       func(Context, RecordWriter) error
//	Reducers     int
//	Reducer      func(Context, RecordReader) error
//	Memory       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobMR) Run() error {
//	buffer, err := newBuffer(j.Mappers, j.Reducers, j.TempDir, j.Memory)
//	if err != nil {
//		return err
//	}
//	return run(
//		j.Mappers,
//		func(c Context) error {
//			w := newRecordWriter(c, buffer)
//			return j.Mapper(c, w)
//		},
//		j.Reducers,
//		func(c Context) error {
//			r := newRecordReader(c, buffer)
//			return j.Reducer(c, r)
//		},
//		j.Memory,
//		j.TempDir,
//		j.LoggerOutput,
//	)
//}
//
//// JobIMR ...
//type JobIMR struct {
//	Mappers      int
//	Mapper       func(Context, io.Reader, RecordWriter) error
//	Reducers     int
//	Reducer      func(Context, RecordReader) error
//	Input        string
//	Memory       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobIMR) Run() error {
//	return nil
//}
//
//// JobMRO ...
//type JobMRO struct {
//	Mappers      int
//	Mapper       func(Context, RecordWriter) error
//	Reducers     int
//	Reducer      func(Context, RecordReader, io.Writer) error
//	Output       string
//	Memory       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobMRO) Run() error {
//	return nil
//}
//
//// JobIMRO ...
//type JobIMRO struct {
//	Mappers      int
//	Mapper       func(Context, io.Reader, RecordWriter) error
//	Reducers     int
//	Reducer      func(Context, RecordReader, io.Writer) error
//	Input        string
//	Output       string
//	Memory       string
//	TempDir      string
//	LoggerOutput io.Writer
//}
//
//// Run ...
//func (j JobIMRO) Run() error {
//	return nil
//}

// Context ...
type Context interface {

	// WorkerID returns a unique number in the [0,mappers) range for mapper
	// workers and a unique number in the [0,reducers) range for reducer
	// workers.
	WorkerID() int

	// Scratch returns a temporary directory that will be automatically removed
	// after the xrt job terminates. It can be used to store any temporary data
	// that should be deleted once the job is done. The scratch directory is
	// scoped to the xrt job, not the individual worker. Thus, any writes by
	// multiple workers needs to be coordinated (generally this is done by
	// naming files using the WorkerID).
	Scratch() string

	// Logger returns a logger that is scoped to this worker. Any messages
	// logged through this logger will appear in the xrt log decorated with the
	// WorkerID.
	Logger() *log.Logger

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

type context struct {
	workerID int
	mappers  int
	reducers int
	scratch  string
	logger   Logger
	done     chan struct{}
}

// Done implements the context pattern and can be used by workers if they
// should abandon any current work since one or many errors have occured in
// other workers.
func (c *context) Done() <-chan struct{} {
	return c.done
}

func (c *context) cancel() {
	close(c.done)
}

func newContext(done chan struct{}) *context {
	return Context{
		done: done,
	}
}

// Main ...
//func Main(mapper MapperFunc, reducer ReducerFunc, exit ExitFunc) {
//	conf, err := configParse(os.Args)
//	if err != nil {
//		fmt.Println(err)
//		os.Exit(1)
//	}
//	if conf.showHelp {
//		configHelp()
//		os.Exit(1)
//	}
//	if conf.showVersion {
//		fmt.Println(Version)
//		return
//	}
//	run(conf, mapper, reducer, exit)
//}

func run(conf *config, mapper MapperFunc, reducer ReducerFunc, exit ExitFunc) {
	startTime := time.Now()
	if err := configFileSystem(conf); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var once sync.Once
	startInterruptHandler(once, exit)
	log.Print("")
	log.Print("                                                 tttt")
	log.Print("                                              ttt:::t")
	log.Print("                                              t:::::t")
	log.Print("                                              t:::::t")
	log.Print("xxxxxxx      xxxxxxxrrrrr   rrrrrrrrr   ttttttt:::::ttttttt")
	log.Print(" x:::::x    x:::::x r::::rrr:::::::::r  t:::::::::::::::::t")
	log.Print("  x:::::x  x:::::x  r:::::::::::::::::r t:::::::::::::::::t")
	log.Print("   x:::::xx:::::x   rr::::::rrrrr::::::rtttttt:::::::tttttt")
	log.Print("    x::::::::::x     r:::::r     r:::::r      t:::::t")
	log.Print("     x::::::::x      r:::::r     rrrrrrr      t:::::t")
	log.Print("     x::::::::x      r:::::r                  t:::::t")
	log.Print("    x::::::::::x     r:::::r                  t:::::t    tttttt")
	log.Print("   x:::::xx:::::x    r:::::r                  t::::::tttt:::::t")
	log.Print("  x:::::x  x:::::x   r:::::r                  tt::::::::::::::t")
	log.Print(" x:::::x    x:::::x  r:::::r                    tt:::::::::::tt")
	log.Print("xxxxxxx      xxxxxxx rrrrrrr                      ttttttttttt")
	log.Print("")
	log.Print("===============================================================")
	log.Printf("version: %s", version)
	log.Print("===============================================================")
	log.Print("")
	log.Print("configuration:")
	log.Print("")
	if conf.hasInput() {
		log.Printf("  input:    %s", conf.input)
	} else {
		log.Print("  input:    (none)")
	}
	log.Printf("  mapper:   %s", conf.mapper)
	log.Printf("  mappers:  %d", conf.mappers)
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
	log.Print("")
	log.Print("running mapper stage")
	log.Print("")
	startTimeMappers := time.Now()
	buffers, err := runMappers(conf, mapper)
	if err != nil {
		rollback(once, exit, err)
	}
	durationMappers := time.Since(startTimeMappers)
	log.Print("")
	//var durationReducers time.Duration
	//if hasReducer() {
	//	log.Print("running reducer stage")
	//	log.Print("")
	//	startTimeReducers := time.Now()
	//	if err := runReducers(conf, reducers, buffers); err != nil {
	//		rollback(once, exit, err)
	//	}
	//	durationReducers = time.Since(startTimeReducers)
	//	log.Print("")
	//}
	//if hasOutput() {
	//	log.Print("committing")
	//	log.Print("")
	//	commit()
	//}
	//log.Printf("  mappers runtime: %s", durationMappers.String())
	//if hasReducer() {
	//	log.Printf("  reducers runtime: %s", durationReducers.String())
	//}
	//log.Printf("  total runtime: %s", time.Since(startTime).String())
	//log.Print("")
	//log.Print("success")
	//if !hasOutput() {
	//	printOutput()
	//}
	//cleanup()
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
