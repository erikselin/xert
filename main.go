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
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

const (
	argInput        = "input"
	argMapper       = "mapper"
	argMappers      = "mappers"
	argMemoryString = "memory"
	argOutput       = "output"
	argProfile      = "profile"
	argReducer      = "reducer"
	argReducers     = "reducers"
	argShowVersion  = "version"
	argTempDir      = "tempdir"
)

var (
	// cli flag defaults
	defaultMappers      = 4
	defaultReducers     = 4
	defaultMemoryString = "256m"
	defaultTempDir      = os.TempDir()

	// set by ldflags at compile time
	version = "unknown"

	// set by cli flags
	mappers      int
	reducers     int
	memoryString string
	tempDir      string
	input        string
	mapper       string
	output       string
	profile      string
	reducer      string
	showVersion  bool

	// computed from cli flags
	memory     int
	tempSpill  string
	tempOutput string

	// inputChunks is a channel from which multiple mapper workers will pull input chunks.
	inputChunks chan *chunk

	// buffers is a matrix of buffers with reducer-rows and mappers-columns partitioning the
	// allocated memory to ensure that it can be accessed without any locking during mapping and
	// reducing. In particular, mapper[i] will write to all buffers in buffers[i][*] while
	// reducer[j] will read from all buffers in buffers[*][j].
	//
	//                          0   1   2
	//                        +---+---+---+
	//                      0 |b00|b01|b02|
	//                        +---+---+---+
	// mapper[1] - write -> 1 |b10|b11|b12|
	//                        +---+---+---+
	//                      2 |b20|b21|b22|
	//                        +---+---+---+
	//                              |
	//                              +- read -> reducer[1]
	buffers [][]*buffer

	// rollbackOnce ensures that we only execute the rollback logic once.
	rollbackOnce sync.Once

	// startTime is used to calculate the total duration of a job.
	startTime = time.Now()
)

func init() {
	flag.StringVar(&input, argInput, "", "")
	flag.StringVar(&mapper, argMapper, "", "")
	flag.IntVar(&mappers, argMappers, defaultMappers, "")
	flag.StringVar(&memoryString, argMemoryString, defaultMemoryString, "")
	flag.StringVar(&output, argOutput, "", "")
	flag.StringVar(&profile, argProfile, "", "")
	flag.StringVar(&reducer, argReducer, "", "")
	flag.IntVar(&reducers, argReducers, defaultReducers, "")
	flag.BoolVar(&showVersion, argShowVersion, false, "")
	flag.StringVar(&tempDir, argTempDir, defaultTempDir, "")
	flag.Usage = usage
}

func main() {
	flag.Parse()
	if len(os.Args) <= 1 {
		usage()
		os.Exit(1)
	}
	if showVersion {
		fmt.Println(version)
		return
	}
	if err := setup(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	run()
}

func usage() {
	fmt.Printf("usage: xrt [--help] [--%s] <options>\n", argShowVersion)
	fmt.Printf(" --%s <input>   the input file or directory\n", argInput)
	fmt.Printf(" --%s <cmd>    mapper command (required)\n", argMapper)
	fmt.Printf(" --%s <num>   number of mappers (default: %d)\n", argMappers, defaultMappers)
	fmt.Printf(" --%s <mem>    memory limit (default: %s)\n", argMemoryString, defaultMemoryString)
	fmt.Printf(" --%s <dir>    output directory\n", argOutput)
	fmt.Printf(" --%s <file>  profile file\n", argProfile)
	fmt.Printf(" --%s <cmd>   reducer command\n", argReducer)
	fmt.Printf(" --%s <num>  number of reducers (default: %d)\n", argReducers, defaultReducers)
	fmt.Printf(" --%s <dir>   temporary directory (default : %s)\n", argTempDir, defaultTempDir)
}

func setup() (err error) {
	if mappers <= 0 {
		return fmt.Errorf("xrt: invalid argument --%s=%d", argMappers, mappers)
	}
	if !hasMapper() {
		return fmt.Errorf("xrt: --%s is required", argMapper)
	}
	if hasReducer() && reducers <= 0 {
		return fmt.Errorf("xrt: invalid argument --%s=%d", argReducers, reducers)
	}
	if memory = parseMemory(memoryString); memory < 0 {
		return fmt.Errorf("xrt: invalid argument --%s=%s", argMemoryString, memoryString)
	}
	if tempDir, err = ioutil.TempDir(tempDir, "xrt-"); err != nil {
		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempDir, err)
	}
	tempOutput = path.Join(tempDir, "output")
	if err = os.Mkdir(tempOutput, 0700); err != nil {
		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempOutput, err)
	}
	tempSpill = path.Join(tempDir, "spill")
	if err = os.Mkdir(tempSpill, 0700); err != nil {
		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempSpill, err)
	}
	if _, err := os.Stat(output); hasOutput() && err == nil {
		return fmt.Errorf("xrt: --%s directory %s already exists", argOutput, output)
	}
	if hasInput() {
		if inputChunks, err = enumerateChunks(input); err != nil {
			return fmt.Errorf("parsing --%s failed with error: %v", argInput, err)
		}
	}
	if hasReducer() {
		buffers = make([][]*buffer, mappers)
		for i := range buffers {
			buffers[i] = make([]*buffer, reducers)
		}
	}
	return nil
}

func run() {
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
	log.Printf("version: %s", version)
	log.Print("===============================================================")
	log.Print("")
	log.Print("configuration:")
	log.Print("")
	log.Printf("  mappers: %d", mappers)
	log.Printf("  reducers: %d", reducers)
	log.Printf("  memory: %s", memoryString)
	log.Printf("  temporary directory: %s", tempDir)
	log.Print("")
	log.Print("plan:")
	log.Print("")
	indent := "  "
	if len(output) > 0 {
		log.Printf("%s->  output (%s)", indent, output)
		indent = indent + "  "
	}
	if len(reducer) > 0 {
		log.Printf("%s->  reduce (%s)", indent, reducer)
		indent = indent + "  "
		log.Printf("%s->  partition and sort", indent)
		indent = indent + "  "
	}
	log.Printf("%s->  map (%s)", indent, mapper)
	if len(input) > 0 {
		log.Printf("%s  ->  input (%s)", indent, input)
	}
	log.Print("")
	log.Print("running mapper stage")
	log.Print("")
	startTimeMappers := time.Now()
	if err := runMany(mappers, mapWorker); err != nil {
		rollback(err)
	}
	durationMappers := time.Since(startTimeMappers)
	log.Print("")
	var durationReducers time.Duration
	if len(reducer) > 0 {
		log.Print("running reducer stage")
		log.Print("")
		startTimeReducers := time.Now()
		if err := runMany(reducers, reduceWorker); err != nil {
			rollback(err)
		}
		durationReducers = time.Since(startTimeReducers)
		log.Print("")
	}
	log.Print("committing")
	log.Print("")
	commit()
	log.Printf("  mappers runtime: %s", durationMappers.String())
	if len(reducer) > 0 {
		log.Printf("  reducers runtime: %s", durationReducers.String())
	}
	log.Printf("  total runtime: %s", time.Since(startTime).String())
	log.Print("")
	log.Print("success")
}

// startInterruptHandler launches a handler that will catch the first interrupt signal and attempt
// a graceful termination (mainly to deal with ctrl-c)
func startInterruptHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		rollback(errors.New("received interrupt - aborting job"))
	}()
}

func runMany(workers int, worker func(context) error) error {
	errc := make(chan error)
	for i := 0; i < workers; i++ {
		go func(wid int) { errc <- worker(context{wid, mappers, reducers}) }(i)
	}
	for i := 0; i < workers; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}
	return nil
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

func mapStdinHandler(c context, w io.WriteCloser) error {
	if hasInput() {
		return inputStream(c, w, inputChunks)
	}
	return w.Close()
}

func mapStdoutHandler(c context, r io.ReadCloser) error {
	if hasReducer() {
		return intermediateMapStream(c, r, buffers[c.workerID])
	}
	if hasOutput() {
		return outputStream(c, r, tempOutput)
	}
	return closedStream(c, r)
}

func reduceWorker(c context) error {
	c.log("reducer starting")
	defer c.log("done")
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
	if hasOutput() {
		return outputStream(c, r, tempOutput)
	}
	return closedStream(c, r)
}

// rollback ensure graceful termination of a failed job. It kills and running mapper or reducer
// commands, ensures no more are spawned and removes any temorary data.
func rollback(err error) {
	rollbackOnce.Do(func() {
		log.Print(err)
		killAll()
		// BUG this will break on windows since it does not allow removal of open files and by the
		// time this is called it is possible fds in the tempdir are still open.
		if err := os.RemoveAll(tempDir); err != nil {
			log.Printf("failed to remove temporary data directory %s - %v", tempDir, err)
		}
		log.Print("failed")
		os.Exit(1)
	})
}

// commit ensures transactional termination of succesfull jobs. If the job is configured to create
// output commit uses a directory move to transactioanlly "commit" the output from a temporary
// folder to the final output folder. commit also cleans up any temporary files.
func commit() {
	if hasOutput() {
		if err := os.Rename(tempOutput, output); err != nil {
			log.Printf("  error moving output data from %s to %s - %v", tempOutput, output, err)
			log.Printf("  temporary data directory %s was not removed", tempDir)
			log.Print("failed")
			os.Exit(1)
		}
	}
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
