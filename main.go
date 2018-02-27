package main

import (
	"errors"
	"flag"
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

const (
	argMappers  = "mappers"
	argReducers = "reducers"
	argMemory   = "memory"
	argTempDir  = "tempdir"

	argInput   = "input"
	argMapper  = "mapper"
	argOutput  = "output"
	argReducer = "reducer"

	argVersion = "version"
)

var (
	// start time of xrt
	start = time.Now()

	// resource defaults
	defaultMappers  = 4
	defaultReducers = 4
	defaultMemory   = "256m"
	defaultTempDir  = os.TempDir()

	// set by ldflags at compile time
	buildVersion = "unknown"
	buildSha     = "unknown"

	// set by cli flags or env variables
	mappers      int
	reducers     int
	memoryString string
	tempDir      string

	// set by cli flags
	input       string
	mapper      string
	output      string
	reducer     string
	showVersion bool

	// parsed from cli flags
	memory     int
	tempSpill  string
	tempOutput string

	// channel for input splits
	splits chan *split

	// buffers for shuffle and sort
	buffers [][]*buffer

	// once primitive to ensure we only call rollback once
	once sync.Once
)

func init() {
	flag.IntVar(&mappers, argMappers, defaultMappers, "")
	flag.IntVar(&reducers, argReducers, defaultReducers, "")
	flag.StringVar(&memoryString, argMemory, defaultMemory, "")
	flag.StringVar(&tempDir, argTempDir, defaultTempDir, "")

	flag.StringVar(&input, argInput, "", "")
	flag.StringVar(&mapper, argMapper, "", "")
	flag.StringVar(&output, argOutput, "", "")
	flag.StringVar(&reducer, argReducer, "", "")

	flag.BoolVar(&showVersion, argVersion, false, "")

	flag.Usage = func() {
		fmt.Print(`Usage: xrt [--help] [--version] <options>

Resource options:
 --mappers <num>   set the number of mappers to <n> (default: 4)
 --reducers <num>  set the number of reducers to <n> (default: 4)
 --memory <mem>    set the amount of allocated memory (default: 256m)
 --tempdir <dir>   set the temporary directory (default : system temporary directory)

Job options:
 --input <file|dir>  the input file or directory
 --mapper <cmd>      mapper command (required)
 --output <dir>      output directory
 --reducer <cmd>     reducer command
`)
	}
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Println(versionString())
		return
	}

	if err := setup(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	startInterruptHandler()

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
	log.Printf("version %s", versionString())
	log.Print("===============================================================")
	log.Print("")

	logConfig()
	log.Print("")

	logPlan()
	log.Print("")

	mapperStart := time.Now()
	log.Print("running mapper stage")
	log.Print("")
	if err := runMany(mappers, mapperWorker); err != nil {
		rollback(err)
	}
	log.Print("")
	mapperDuration := time.Since(mapperStart)

	reducerStart := time.Now()
	if hasReducer() {
		log.Print("running reducer stage")
		log.Print("")
		if err := runMany(reducers, reducerWorker); err != nil {
			rollback(err)
		}
		log.Print("")
	}
	reducerDuration := time.Since(reducerStart)

	log.Print("finalizing")
	log.Print("")
	commit()

	log.Printf("  mappers runtime: %s", mapperDuration.String())
	if hasReducer() {
		log.Printf("  reducers runtime: %s", reducerDuration.String())
	}
	log.Printf("  total runtime: %s", time.Since(start).String())
	log.Print("")
	log.Print("success")
}

// setup ...
func setup() error {
	var err error

	if mappers <= 0 {
		return fmt.Errorf("xrt: invalid argument -%s=%d", argMappers, mappers)
	}

	if reducers <= 0 {
		return fmt.Errorf("xrt: invalid argument -%s=%d", argReducers, reducers)
	}

	if !hasMapper() {
		return fmt.Errorf("xrt: -%s is required", argMapper)
	}

	if memory, err = parseMemory(memoryString); err != nil {
		return err
	}

	if tempDir, err = ioutil.TempDir(tempDir, "xrt-"); err != nil {
		return fmt.Errorf("xrt: bad directory '%s' - %v", tempDir, err)
	}

	tempOutput = path.Join(tempDir, "output")
	if err := os.Mkdir(tempOutput, 0700); err != nil {
		return err
	}

	tempSpill = path.Join(tempDir, "spill")
	if err := os.Mkdir(tempSpill, 0700); err != nil {
		return err
	}

	if splits, err = enumerate(input); err != nil {
		return fmt.Errorf("parsing -%s failed with error: %v", argInput, err)
	}

	buffers = make([][]*buffer, mappers)
	for i := range buffers {
		buffers[i] = make([]*buffer, reducers)
	}

	if hasOutput() {
		if _, err := os.Stat(output); err == nil {
			return fmt.Errorf(
				"-%s directory %s already exists",
				argOutput,
				output,
			)
		}
	}

	return nil
}

// parseMemory takes a string representing a memory amount and converts it into
// a integer representign the number of bytes.
// For example:
//    parseMemory("1k") = 1024
//    parseMemory("1k") = 1048576
// a error is returned if a bad memory string was provided.
func parseMemory(v string) (int, error) {
	var m uint

	switch v[len(v)-1] {
	case 'b':
		m = 0
	case 'k':
		m = 10
	case 'm':
		m = 20
	case 'g':
		m = 30
	case 't':
		m = 40
	case 'p':
		m = 50
	default:
		return 0, fmt.Errorf("xrt: invalid argument -%s=%s", argMemory, v)
	}

	n, err := strconv.Atoi(v[0 : len(v)-1])
	if err != nil {
		return 0, fmt.Errorf("xrt: invalid argument -%s=%s", argMemory, v)
	}

	return n << m, nil
}

// startInterruptHandler launches a handler that will catch the first interrupt
// signal and attempt a graceful termination (mainly to deal with ctrl-c)
func startInterruptHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		rollback(errors.New("received interrupt - aborting job"))
	}()
}

// versionString builds and returns the version string
func versionString() string {
	return fmt.Sprintf("%s-%s", buildVersion, buildSha)
}

// logConfig prints information about the job
func logConfig() {
	log.Print("configuration:")
	log.Print("")
	log.Printf("  mappers: %d", mappers)
	log.Printf("  reducers: %d", reducers)
	log.Printf("  memory: %s", memoryString)
	log.Printf("  temporary directory: %s", tempDir)
}

// logPlan prints the execution plan for the job
func logPlan() {
	log.Print("plan:")
	log.Print("")

	indent := "  "
	if hasOutput() {
		log.Printf("%s->  output (%s)", indent, output)
		indent = indent + "  "
	}

	if hasReducer() {
		log.Printf("%s->  reduce (%s)", indent, reducer)
		indent = indent + "  "
		log.Printf("%s->  shuffle and sort", indent)
		indent = indent + "  "
	}

	log.Printf("%s->  map (%s)", indent, mapper)
	if hasInput() {
		log.Printf("%s  ->  input (%s)", indent, input)
	}
}

// mapperWorker is the mapper stage worker. It is responsible for running worker
// specific code, running the user provided commands and ensure the streams are
// handled according to the job configuration.
func mapperWorker(c *context) error {
	c.log("mapper starting")
	defer c.log("done")

	if hasReducer() {
		bufMem := memory / (mappers * reducers)
		for i := range buffers[c.workerID] {
			buffers[c.workerID][i] = newBuffer(c.workerID, i, bufMem, tempSpill)
		}
	}

	if err := c.exec(
		mapper,
		mapperStdin,
		mapperStdout,
		streamToLog,
	); err != nil {
		return err
	}

	if hasReducer() {
		c.log("sorting...")
		for _, b := range buffers[c.workerID] {
			b.sort()
			if b.spills > 1 {
				c.log("sorting spills...")
			}
			if err := b.externalSort(); err != nil {
				return err
			}
		}
	}

	return nil
}

func mapperStdin(c *context, w io.WriteCloser) error {
	if hasInput() {
		return streamFromInput(c, w, splits)
	}
	return w.Close()
}

func mapperStdout(c *context, r io.ReadCloser) error {
	if hasReducer() {
		return streamToShuffleSort(c, r, buffers[c.workerID])
	}
	if hasOutput() {
		return streamToOutput(c, r, tempOutput)
	}
	return streamClosed(c, r)
}

// reducerWorker is the reducer stage worker. It is responsible for running
// worker specific code, running the user provided commands and ensure the
// streams are handled according to the job configuration.
func reducerWorker(c *context) error {
	c.log("reducer starting")
	defer c.log("done")

	return c.exec(
		reducer,
		reducerStdin,
		reducerStdout,
		streamToLog,
	)
}

func reducerStdin(c *context, w io.WriteCloser) error {
	workerBuffers := make([]*buffer, len(buffers))
	for i := range buffers {
		workerBuffers[i] = buffers[i][c.workerID]
	}
	return streamFromShuffleSort(c, w, workerBuffers)
}

func reducerStdout(c *context, r io.ReadCloser) error {
	if hasOutput() {
		return streamToOutput(c, r, tempOutput)
	}
	return streamClosed(c, r)
}

// rollback ensure graceful termination of a failed job. It kills and running
// mapper or reducer commands, ensures no more are spawned and removes any
// temorary data.
func rollback(err error) {
	once.Do(func() {
		log.Print(err)

		killAll()

		// BUG this will break on windows since it does not allow removal of
		//     open files and by the time this is called it is possible fds in
		//     the tempdir are still open.
		if err := os.RemoveAll(tempDir); err != nil {
			log.Print(
				"failed to remove temporary data directory %s - %v",
				tempDir,
				err,
			)
		}

		log.Print("failed")
		os.Exit(1)
	})
}

// commit ensures transactional termination of succesfull jobs. If the job is
// configured to create output commit uses a directory move to transactioanlly
// "commit" the output from a temporary folder to the final output folder.
// commit also cleans up any temporary files.
func commit() {
	if hasOutput() {
		if err := os.Rename(tempOutput, output); err != nil {
			log.Printf(
				"  error while moving output data from %s to %s - %v",
				tempOutput,
				output,
				err,
			)
			log.Printf("  temporary data directory %s was not removed", tempDir)
			log.Print("failed")
			os.Exit(1)
		}
	}

	if err := os.RemoveAll(tempDir); err != nil {
		log.Print(
			"  failed to remove temporary data directory %s - %v",
			tempDir,
			err,
		)
	}
}

// hasInput returns true if input is set
func hasInput() bool {
	return len(input) > 0
}

// hasMapper returns true if mapper is set
func hasMapper() bool {
	return len(mapper) > 0
}

// hasOutput returns true if Output is set
func hasOutput() bool {
	return len(output) > 0
}

// hasReducer returns true if Reducer is set
func hasReducer() bool {
	return len(reducer) > 0
}
