package core

//import (
//	"bufio"
//	"errors"
//	"flag"
//	"fmt"
//	"io"
//	"io/ioutil"
//	"log"
//	"os"
//	"os/signal"
//	"path"
//	"runtime/pprof"
//	"strconv"
//	"sync"
//	"time"
//)
//
//const (
//	argInput        = "input"
//	argMapper       = "mapper"
//	argMappers      = "mappers"
//	argMemoryString = "memory"
//	argOutput       = "output"
//	argProfile      = "profile"
//	argReducer      = "reducer"
//	argReducers     = "reducers"
//	argShowVersion  = "version"
//	argTempDir      = "tempdir"
//)
//
//var (
//	// cli flag defaults
//	defaultMappers      = 1
//	defaultReducers     = 1
//	defaultMemoryString = "16m"
//	defaultTempDir      = os.TempDir()
//
//	// set by ldflags at compile time
//	version = "unknown"
//
//	// set by cli flags
//	mappers      int
//	reducers     int
//	memoryString string
//	tempDir      string
//	input        string
//	mapper       string
//	output       string
//	profile      string
//	reducer      string
//	showVersion  bool
//
//	// computed from cli flags
//	memory     int
//	tempSpill  string
//	tempOutput string
//
//	// inputChunks is a channel from which multiple mapper workers will pull input chunks.
//	inputChunks chan *chunk
//
//	// buffers is a matrix of buffers with reducer-rows and mappers-columns partitioning the
//	// allocated memory to ensure that it can be accessed without any locking during mapping and
//	// reducing. In particular, mapper[i] will write to all buffers in buffers[i][*] while
//	// reducer[j] will read from all buffers in buffers[*][j].
//	//
//	//                          0   1   2
//	//                        +---+---+---+
//	//                      0 |b00|b01|b02|
//	//                        +---+---+---+
//	// mapper[1] - write -> 1 |b10|b11|b12|
//	//                        +---+---+---+
//	//                      2 |b20|b21|b22|
//	//                        +---+---+---+
//	//                              |
//	//                              +- read -> reducer[1]
//	buffers [][]*buffer
//
//	// rollbackOnce ensures that we only execute the rollback logic once.
//	rollbackOnce sync.Once
//
//	// startTime is used to calculate the total duration of a job.
//	startTime = time.Now()
//)
//
//func init() {
//	flag.StringVar(&input, argInput, "", "")
//	flag.StringVar(&mapper, argMapper, "", "")
//	flag.IntVar(&mappers, argMappers, defaultMappers, "")
//	flag.StringVar(&memoryString, argMemoryString, defaultMemoryString, "")
//	flag.StringVar(&output, argOutput, "", "")
//	flag.StringVar(&profile, argProfile, "", "")
//	flag.StringVar(&reducer, argReducer, "", "")
//	flag.IntVar(&reducers, argReducers, defaultReducers, "")
//	flag.BoolVar(&showVersion, argShowVersion, false, "")
//	flag.StringVar(&tempDir, argTempDir, defaultTempDir, "")
//	flag.Usage = usage
//}
//
//func main() {
//	flag.Parse()
//	if len(os.Args) <= 1 {
//		usage()
//		os.Exit(1)
//	}
//	if showVersion {
//		fmt.Println(version)
//		return
//	}
//	if err := setup(); err != nil {
//		fmt.Println(err)
//		os.Exit(1)
//	}
//	if profile != "" {
//		f, err := os.Create(profile)
//		if err != nil {
//			fmt.Println(err)
//			os.Exit(1)
//		}
//		pprof.StartCPUProfile(f)
//		defer pprof.StopCPUProfile()
//	}
//	run()
//}
//
//func usage() {
//	fmt.Printf("usage: xrt [--help] [--%s] <arguments>\n", argShowVersion)
//	fmt.Printf(" --%s <input>   Input pattern, example: path/to/file_*.tsv\n", argInput)
//	fmt.Printf(" --%s <cmd>    Mapper command (required)\n", argMapper)
//	fmt.Printf(" --%s <num>   Number of mappers (default: %d)\n", argMappers, defaultMappers)
//	fmt.Printf(" --%s <mem>    Memory limit, example: 1k, 2m, 3g, 4t (default: %s)\n", argMemoryString, defaultMemoryString)
//	fmt.Printf(" --%s <dir>    Output directory, if not set any output will go to stdout\n", argOutput)
//	fmt.Printf(" --%s <cmd>   Reducer command, do not set for a map-only job\n", argReducer)
//	fmt.Printf(" --%s <num>  Number of reducers (default: %d)\n", argReducers, defaultReducers)
//	fmt.Printf(" --%s <dir>   Temporary directory (default : %s)\n", argTempDir, defaultTempDir)
//}
//
//func setup() (err error) {
//	if mappers <= 0 {
//		return fmt.Errorf("xrt: invalid argument --%s=%d", argMappers, mappers)
//	}
//	if !hasMapper() {
//		return fmt.Errorf("xrt: --%s is required", argMapper)
//	}
//	if hasReducer() && reducers <= 0 {
//		return fmt.Errorf("xrt: invalid argument --%s=%d", argReducers, reducers)
//	}
//	if memory = parseMemory(memoryString); memory < 0 {
//		return fmt.Errorf("xrt: invalid argument --%s=%s", argMemoryString, memoryString)
//	}
//	if tempDir, err = ioutil.TempDir(tempDir, "xrt-"); err != nil {
//		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempDir, err)
//	}
//	tempOutput = path.Join(tempDir, "output")
//	if err = os.Mkdir(tempOutput, 0700); err != nil {
//		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempOutput, err)
//	}
//	tempSpill = path.Join(tempDir, "spill")
//	if err = os.Mkdir(tempSpill, 0700); err != nil {
//		return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempSpill, err)
//	}
//	if _, err := os.Stat(output); hasOutput() && err == nil {
//		return fmt.Errorf("xrt: --%s directory %s already exists", argOutput, output)
//	}
//	if hasInput() {
//		if inputChunks, err = enumerateChunks(input); err != nil {
//			return fmt.Errorf("parsing --%s failed with error: %v", argInput, err)
//		}
//	}
//	if hasReducer() {
//		buffers = make([][]*buffer, mappers)
//		for i := range buffers {
//			buffers[i] = make([]*buffer, reducers)
//		}
//	}
//	return nil
//}
//
//// parseMemory takes a string representing a memory amount and converts it into
//// a integer representign the number of bytes.
//// For example:
////    parseMemory("1k") = 1024
////    parseMemory("1k") = 1048576
//// -1 is returned if a bad memory string was provided.
//func parseMemory(v string) int {
//	var m uint
//	switch v[len(v)-1] {
//	case 'b':
//		m = 0
//	case 'k':
//		m = 10
//	case 'm':
//		m = 20
//	case 'g':
//		m = 30
//	case 't':
//		m = 40
//	case 'p':
//		m = 50
//	default:
//		return -1
//	}
//	n, err := strconv.Atoi(v[0 : len(v)-1])
//	if err != nil {
//		return -1
//	}
//	return n << m
//}
//
//func run() {
//	startInterruptHandler()
//	log.Print("")
//	log.Print("                                                 tttt")
//	log.Print("                                              ttt:::t")
//	log.Print("                                              t:::::t")
//	log.Print("                                              t:::::t")
//	log.Print("xxxxxxx      xxxxxxxrrrrr   rrrrrrrrr   ttttttt:::::ttttttt")
//	log.Print(" x:::::x    x:::::x r::::rrr:::::::::r  t:::::::::::::::::t")
//	log.Print("  x:::::x  x:::::x  r:::::::::::::::::r t:::::::::::::::::t")
//	log.Print("   x:::::xx:::::x   rr::::::rrrrr::::::rtttttt:::::::tttttt")
//	log.Print("    x::::::::::x     r:::::r     r:::::r      t:::::t")
//	log.Print("     x::::::::x      r:::::r     rrrrrrr      t:::::t")
//	log.Print("     x::::::::x      r:::::r                  t:::::t")
//	log.Print("    x::::::::::x     r:::::r                  t:::::t    tttttt")
//	log.Print("   x:::::xx:::::x    r:::::r                  t::::::tttt:::::t")
//	log.Print("  x:::::x  x:::::x   r:::::r                  tt::::::::::::::t")
//	log.Print(" x:::::x    x:::::x  r:::::r                    tt:::::::::::tt")
//	log.Print("xxxxxxx      xxxxxxx rrrrrrr                      ttttttttttt")
//	log.Print("")
//	log.Print("===============================================================")
//	log.Printf("version: %s", version)
//	log.Print("===============================================================")
//	log.Print("")
//	log.Print("configuration:")
//	log.Print("")
//	log.Printf("  mappers: %d", mappers)
//	if hasReducer() {
//		log.Printf("  reducers: %d", reducers)
//	}
//	log.Printf("  memory: %s", memoryString)
//	log.Printf("  temporary directory: %s", tempDir)
//	log.Print("")
//	log.Print("plan:")
//	log.Print("")
//	indent := "  "
//	if hasOutput() {
//		log.Printf("%s->  output (%s)", indent, output)
//		indent = indent + "  "
//	}
//	if hasReducer() {
//		log.Printf("%s->  reduce (%s)", indent, reducer)
//		indent = indent + "  "
//		log.Printf("%s->  partition and sort", indent)
//		indent = indent + "  "
//	}
//	log.Printf("%s->  map (%s)", indent, mapper)
//	if hasInput() {
//		log.Printf("%s  ->  input (%s)", indent, input)
//	}
//	log.Print("")
//	log.Print("running mapper stage")
//	log.Print("")
//	startTimeMappers := time.Now()
//	if err := runMany(mappers, mapWorker); err != nil {
//		rollback(err)
//	}
//	durationMappers := time.Since(startTimeMappers)
//	log.Print("")
//	var durationReducers time.Duration
//	if hasReducer() {
//		// TODO:
//		// log.Print("intermediate record distribution:"
//		// log.Print("")
//		log.Print("running reducer stage")
//		log.Print("")
//		startTimeReducers := time.Now()
//		if err := runMany(reducers, reduceWorker); err != nil {
//			rollback(err)
//		}
//		durationReducers = time.Since(startTimeReducers)
//		log.Print("")
//	}
//	if hasOutput() {
//		log.Print("committing")
//		log.Print("")
//		commit()
//	}
//	log.Printf("  mappers runtime: %s", durationMappers.String())
//	if hasReducer() {
//		log.Printf("  reducers runtime: %s", durationReducers.String())
//	}
//	log.Printf("  total runtime: %s", time.Since(startTime).String())
//	log.Print("")
//	log.Print("success")
//	if !hasOutput() {
//		printOutput()
//	}
//	cleanup()
//}
//
//// startInterruptHandler launches a handler that will catch the first interrupt signal and attempt
//// a graceful termination (mainly to deal with ctrl-c)
//func startInterruptHandler() {
//	c := make(chan os.Signal, 1)
//	signal.Notify(c, os.Interrupt)
//	go func() {
//		<-c
//		rollback(errors.New("received interrupt - aborting job"))
//	}()
//}
//
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
//
//func mapWorker(c context) error {
//	c.log("mapper starting")
//	defer c.log("done")
//	if hasReducer() {
//		bufMem := memory / (mappers * reducers)
//		for i := range buffers[c.workerID] {
//			spillDir := path.Join(tempSpill, strconv.Itoa(c.workerID), strconv.Itoa(i))
//			buffers[c.workerID][i] = newBuffer(bufMem, spillDir)
//		}
//	}
//	if err := c.exec(mapper, mapStdinHandler, mapStdoutHandler, logStream); err != nil {
//		return err
//	}
//	if hasReducer() && reducers <= mappers {
//		c.log("sorting")
//		for i, b := range buffers[c.workerID] {
//			b.sort()
//			if b.needExternalSort() {
//				c.logf("merging %d spill files for reducer %d", b.spills, i)
//				if err := b.externalSort(); err != nil {
//					return err
//				}
//			}
//		}
//	}
//	return nil
//}
//
//func mapStdinHandler(c context, w io.WriteCloser) error {
//	if hasInput() {
//		return inputStream(c, w, inputChunks)
//	}
//	return w.Close()
//}
//
//func mapStdoutHandler(c context, r io.ReadCloser) error {
//	if hasReducer() {
//		return intermediateMapStream(c, r, buffers[c.workerID])
//	}
//	return outputStream(c, r, tempOutput)
//}
//
//func reduceWorker(c context) error {
//	c.log("reducer starting")
//	defer c.log("done")
//	if hasReducer() && reducers > mappers {
//		c.log("sorting")
//		for i, b := range buffers[c.workerID] {
//			b.sort()
//			if b.needExternalSort() {
//				c.logf("merging %d spill files for reducer %d", b.spills, i)
//				if err := b.externalSort(); err != nil {
//					return err
//				}
//			}
//		}
//	}
//	records := 0
//	for i := range buffers {
//		records += buffers[i][c.workerID].records
//	}
//	c.logf("processing %d records", records)
//	return c.exec(reducer, reduceStdinHandler, reduceStdoutHandler, logStream)
//}
//
//func reduceStdinHandler(c context, w io.WriteCloser) error {
//	bufs := make([]*buffer, len(buffers))
//	for i := range buffers {
//		bufs[i] = buffers[i][c.workerID]
//	}
//	return intermediateReduceStream(c, w, bufs)
//}
//
//func reduceStdoutHandler(c context, r io.ReadCloser) error {
//	return outputStream(c, r, tempOutput)
//}
//
//// rollback ensure graceful termination of a failed job. It kills and running mapper or reducer
//// commands, ensures no more are spawned and removes any temorary data.
//func rollback(err error) {
//	rollbackOnce.Do(func() {
//		log.Print("error - attempting rollback")
//		log.Print("")
//		log.Print(err)
//		killAll()
//		cleanup()
//		log.Print("failed")
//		os.Exit(1)
//	})
//}
//
//// commit ensures transactional termination of succesfull jobs. If the job is configured to create
//// output commit uses a directory move to transactioanlly "commit" the output from a temporary
//// folder to the final output folder.
//func commit() {
//	if err := os.Rename(tempOutput, output); err != nil {
//		log.Printf("  error moving output data from %s to %s - %v", tempOutput, output, err)
//		log.Printf("  temporary data directory %s was not removed", tempDir)
//		log.Print("failed")
//		os.Exit(1)
//	}
//}
//
//// printOutput reads the output from the temporary directory and copies it to stdout. This is used
//// by runs without output to display the output in the terminal instead of writting it to a file.
//func printOutput() {
//	files, err := ioutil.ReadDir(tempOutput)
//	if err != nil {
//		log.Printf("  error reading output data in %s - %v", tempOutput, err)
//		os.Exit(1)
//	}
//	for _, file := range files {
//		filename := path.Join(tempOutput, file.Name())
//		f, err := os.Open(filename)
//		if err != nil {
//			log.Printf("  error reading output data in %s - %v", filename, err)
//			os.Exit(1)
//		}
//		r := bufio.NewReader(f)
//		w := bufio.NewWriter(os.Stdout)
//		if _, err := io.Copy(w, r); err != nil {
//			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
//		}
//		if err := w.Flush(); err != nil {
//			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
//		}
//	}
//}
//
//// cleanup removes any remaining temporary files.
//func cleanup() {
//	// BUG this will break on windows since it does not allow removal of open files and by the
//	// time this is called it is possible fds in the tempdir are still open.
//	if err := os.RemoveAll(tempDir); err != nil {
//		log.Printf("  failed to remove temporary data directory %s - %v", tempDir, err)
//	}
//}
//
//func hasInput() bool {
//	return len(input) > 0
//}
//
//func hasMapper() bool {
//	return len(mapper) > 0
//}
//
//func hasReducer() bool {
//	return len(reducer) > 0
//}
//
//func hasOutput() bool {
//	return len(output) > 0
//}
