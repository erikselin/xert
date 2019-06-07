// Package xrt ...
//
// Example
//
package xrt

//import (
//	"errors"
//	"fmt"
//	"io"
//	"log"
//	"os"
//	"os/signal"
//	"sync"
//)
//
//// RunM ...
//func RunM(
//	tempDir string,
//	mappers int,
//	mapper func(Context) error,
//	onError func(),
//) error {
//	return run(
//		"",
//		tempDir,
//		"",
//		"",
//		mappers,
//		func(c *context) error {
//			return mapper(c)
//		},
//		0,
//		nil,
//		onError,
//	)
//}
//
//// RunMI ...
//func RunMI(
//	tempDir string,
//	input string,
//	mappers int,
//	mapper func(Context, io.Reader) error,
//	onError func(),
//) error {
//	return run(
//		"",
//		tempDir,
//		input,
//		"",
//		mappers,
//		func(c *context) error {
//			return c.withInputReader(func(r io.Reader) error {
//				return mapper(c, r)
//			})
//		},
//		0,
//		nil,
//		onError,
//	)
//}
//
//// RunMO ...
//func RunMO(
//	tempDir string,
//	output string,
//	mappers int,
//	mapper func(Context, io.Writer) error,
//	onError func(),
//) error {
//	return run(
//		"",
//		tempDir,
//		"",
//		output,
//		mappers,
//		func(c *context) error {
//			return c.withOutputWriter(func(w io.Writer) error {
//				return mapper(c, w)
//			})
//		},
//		0,
//		nil,
//		onError,
//	)
//}
//
//// RunMIO ...
//func RunMIO(
//	tempDir string,
//	input string,
//	output string,
//	mappers int,
//	mapper func(Context, io.Reader, io.Writer) error,
//	onError func(),
//) error {
//	return run(
//		"",
//		tempDir,
//		input,
//		output,
//		mappers,
//		func(c *context) error {
//			return c.withInputReader(func(r io.Reader) error {
//				return c.withOutputWriter(func(w io.Writer) error {
//					return mapper(c, r, w)
//				})
//			})
//		},
//		0,
//		nil,
//		onError,
//	)
//}
//
//// RunMR ...
//func RunMR(
//	memory string,
//	tempDir string,
//	mappers int,
//	mapper func(Context, RecordWriter) error,
//	reducers int,
//	reducer func(Context, RecordReader) error,
//	onError func(),
//) error {
//	return run(
//		memory,
//		tempDir,
//		"",
//		"",
//		mappers,
//		func(c *context) error {
//			return c.withRecordWriter(func(w RecordWriter) error {
//				return mapper(c, w)
//			})
//		},
//		reducers,
//		func(c *context) error {
//			return c.withRecordReader(func(r RecordReader) error {
//				return reducer(c, r)
//			})
//		},
//		onError,
//	)
//}
//
//// RunMRI ...
//func RunMRI(
//	memory string,
//	tempDir string,
//	input string,
//	mappers int,
//	mapper func(Context, io.Reader, RecordWriter) error,
//	reducers int,
//	reducer func(Context, RecordReader) error,
//	onError func(),
//) error {
//	return run(
//		memory,
//		tempDir,
//		input,
//		"",
//		mappers,
//		func(c *context) error {
//			return c.withInputReader(func(r io.Reader) error {
//				return c.withRecordWriter(func(w RecordWriter) error {
//					return mapper(c, r, w)
//				})
//			})
//		},
//		reducers,
//		func(c *context) error {
//			return c.withRecordReader(func(r RecordReader) error {
//				return reducer(c, r)
//			})
//		},
//		onError,
//	)
//}
//
//// RunMRO ...
//func RunMRO(
//	memory string,
//	tempDir string,
//	output string,
//	mappers int,
//	mapper func(Context, RecordWriter) error,
//	reducers int,
//	reducer func(Context, RecordReader, io.Writer) error,
//	onError func(),
//) error {
//	return run(
//		memory,
//		tempDir,
//		"",
//		output,
//		mappers,
//		func(c *context) error {
//			return c.withRecordWriter(func(w RecordWriter) error {
//				return mapper(c, w)
//			})
//		},
//		reducers,
//		func(c *context) error {
//			return c.withRecordReader(func(r RecordReader) error {
//				return c.withOutputWriter(func(w io.Writer) error {
//					return reducer(c, r, w)
//				})
//			})
//		},
//		onError,
//	)
//}
//
//// RunMRIO ...
//func RunMRIO(
//	memory string,
//	tempDir string,
//	input string,
//	output string,
//	mappers int,
//	mapper func(Context, io.Reader, RecordWriter) error,
//	reducers int,
//	reducer func(Context, RecordReader, io.Writer) error,
//	onError func(),
//) error {
//	return run(
//		memory,
//		tempDir,
//		input,
//		output,
//		mappers,
//		func(c *context) error {
//			return c.withInputReader(func(r io.Reader) error {
//				return c.withRecordWriter(func(w RecordWriter) error {
//					return mapper(c, r, w)
//				})
//			})
//		},
//		reducers,
//		func(c *context) error {
//			return c.withRecordReader(func(r RecordReader) error {
//				return c.withOutputWriter(func(w io.Writer) error {
//					return reducer(c, r, w)
//				})
//			})
//		},
//		onError,
//	)
//}
//
//func run(
//	memory string,
//	tempDir string,
//	input string,
//	output string,
//	mappers int,
//	mapper func(*context) error,
//	reducers int,
//	reducer func(*context) error,
//	onError func(),
//) error {
//	conf, err := newConfig(
//		memory,
//		tempDir,
//		input,
//		output,
//		mappers,
//		mapper,
//		reducers,
//		reducer,
//		onError,
//	)
//	if err != nil {
//		return err
//	}
//	log.Print("")
//	log.Print("starting xrt job")
//	log.Print("")
//	log.Print("configuration:")
//	log.Print("")
//	if conf.hasReducer() {
//		log.Printf("  memory:   %s", conf.memoryString)
//	}
//	log.Printf("  tempdir:  %s", conf.tempDir)
//	if conf.hasInput() {
//		log.Printf("  input:    %s", conf.input)
//	}
//	if conf.hasOutput() {
//		log.Printf("  output:   %s", conf.output)
//	}
//	log.Printf("  mappers:  %d", conf.mappers)
//	if conf.hasReducer() {
//		log.Printf("  reducers: %d", conf.reducers)
//	}
//	if err := conf.setupFileSystem(); err != nil {
//		return err
//	}
//	once, signals := startInterruptHandler(conf)
//	defer signal.Stop(signals)
//	log.Print("")
//	log.Print("running mapper stage")
//	log.Print("")
//	//	var in input
//	//	if conf.hasInput() {
//	//		in = newInput(conf.input)
//	//	}
//	//	var out output
//	//	if conf.hasOutput() {
//	//		out = newOutput(conf)
//	//	}
//	//	var buf buffers
//	//	if conf.hasReducer() {
//	//		buf = newBuffer(conf)
//	//	}
//	//	startTimeMappers := time.Now()
//	//	if err := runWorkers(
//	//		conf.mappers,
//	//		func(c *context) error {
//	//			return mapper(c, in, out, buf)
//	//		},
//	//	); err != nil {
//	//		rollback(once, done, err)
//	//	}
//	//	durationMappers := time.Since(startTimeMappers)
//	log.Print("")
//	//	var durationReducers time.Duration
//	if conf.hasReducer() {
//		log.Print("running reducer stage")
//		log.Print("")
//		//		startTimeReducers := time.Now()
//		//		if err := runWorkers(
//		//			conf.reducers,
//		//			func(c *context) error {
//		//				return reducer(c, buf)
//		//			},
//		//		); err != nil {
//		//			rollback(once, done, err)
//		//		}
//		//		durationReducers = time.Since(startTimeReducers)
//		log.Print("")
//	}
//	//	if hasOutput() {
//	//		log.Print("committing")
//	//		log.Print("")
//	//		commit()
//	//	}
//	//	log.Printf("  mappers runtime: %s", durationMappers.String())
//	//	if conf.hasReducer() {
//	//		log.Printf("  reducers runtime: %s", durationReducers.String())
//	//	}
//	//	log.Printf("  total runtime: %s", time.Since(startTime).String())
//	//	log.Print("")
//	//	log.Print("success")
//	return cleanup(conf)
//	// TODO move this to CLI
//	//	if !hasOutput() {
//	//		printOutput()
//	//	}
//}
//
//// startInterruptHandler launches a handler that will catch the first interrupt
//// signal and attempt a graceful termination (mainly to deal with ctrl-c)
//func startInterruptHandler(conf *config) (*sync.Once, chan os.Signal) {
//	var once sync.Once
//	signals := make(chan os.Signal, 1)
//	signal.Notify(signals, os.Interrupt)
//	go func() {
//		<-signals
//		signal.Stop(signals)
//		rollback(conf, &once, errors.New("received interrupt - aborting job"))
//		os.Exit(1)
//	}()
//	return &once, signals
//}
//
//// rollback ensure graceful termination of a failed job by running an optional
//// user provided onError function and removing any data from disk.
//func rollback(conf *config, once *sync.Once, err error) {
//	once.Do(func() {
//		log.Print("error - attempting rollback")
//		log.Print("")
//		log.Print(err)
//		conf.onError()
//		cleanup(conf)
//		log.Print("failed")
//	})
//}
//
//// cleanup removes any remaining temporary files.
//func cleanup(conf *config) error {
//	// BUG this could break on windows since it does not allow removal of open
//	// files and by the time this is called it is possible fds in the tempdir
//	// are still open.
//	if err := os.RemoveAll(conf.tempRoot); err != nil {
//		return fmt.Errorf(
//			"  failed to remove temporary data directory %s - %v",
//			conf.tempRoot,
//			err,
//		)
//	}
//	return nil
//}
//
//////func runMany(workers int, worker func(context) error) error {
//////	errc := make(chan error)
//////	for i := 0; i < workers; i++ {
//////		go func(wid int) { errc <- worker(context{wid, mappers, reducers}) }(i)
//////	}
//////	for i := 0; i < workers; i++ {
//////		if err := <-errc; err != nil {
//////			return err
//////		}
//////	}
//////	return nil
//////}
////func runMappers(conf *config, mapper MapperFunc) ([][]*buffer, error) {
////	in := newInput(conf)
////	out := new
////
////	var buffers [][]*buffer
////	buffers = make([][]*buffer, conf.mappers)
////	for i := range buffers {
////		buffers[i] = make([]*buffer, conf.reducers)
////	}
////
////	if conf.hasOutput() || conf.hasReducer() {
////		if tempDir, err = ioutil.TempDir(conf.tempDir, "xrt-"); err != nil {
////			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempDir, err)
////		}
////	}
////	if conf.hasOutput() {
////		if err = os.Mkdir(tempOutput, 0700); err != nil {
////			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempOutput, err)
////		}
////	}
////	if conf.hasReducer() {
////		if err = os.Mkdir(tempSpill, 0700); err != nil {
////			return fmt.Errorf("xrt: failed initializing directory '%s' - %v", tempSpill, err)
////		}
////	}
////
////	input := newInput(conf)
////	//	errc := make(chan error)
////	//	for i := 0; i < mappers; i++ {
////	//		go func(wid int) {
////	//			l := logger{wid}
////	//			errc <- worker(MapperContext{wid, mapper, mappers, reducers})
////	//		}(i)
////	//	}
////	//	for i := 0; i < workers; i++ {
////	//		if err := <-errc; err != nil {
////	//			return err
////	//		}
////	//	}
////	//	return nil
////}
////
////func mapWorker(c context) error {
////	c.log("mapper starting")
////	defer c.log("done")
////	if hasReducer() {
////		bufMem := memory / (mappers * reducers)
////		for i := range buffers[c.workerID] {
////			spillDir := path.Join(tempSpill, strconv.Itoa(c.workerID), strconv.Itoa(i))
////			buffers[c.workerID][i] = newBuffer(bufMem, spillDir)
////		}
////	}
////	if err := c.exec(mapper, mapStdinHandler, mapStdoutHandler, logStream); err != nil {
////		return err
////	}
////	if hasReducer() && reducers <= mappers {
////		c.log("sorting")
////		for i, b := range buffers[c.workerID] {
////			b.sort()
////			if b.needExternalSort() {
////				c.logf("merging %d spill files for reducer %d", b.spills, i)
////				if err := b.externalSort(); err != nil {
////					return err
////				}
////			}
////		}
////	}
////	return nil
////}
////
////func mapStdinHandler(c context, w io.WriteCloser) error {
////	if hasInput() {
////		return inputStream(c, w, inputs)
////	}
////	return w.Close()
////}
////
////func mapStdoutHandler(c context, r io.ReadCloser) error {
////	if hasReducer() {
////		return intermediateMapStream(c, r, buffers[c.workerID])
////	}
////	return outputStream(c, r, tempOutput)
////}
////
////func reduceWorker(c context) error {
////	c.log("reducer starting")
////	defer c.log("done")
////	if hasReducer() && reducers > mappers {
////		c.log("sorting")
////		for i, b := range buffers[c.workerID] {
////			b.sort()
////			if b.needExternalSort() {
////				c.logf("merging %d spill files for reducer %d", b.spills, i)
////				if err := b.externalSort(); err != nil {
////					return err
////				}
////			}
////		}
////	}
////	records := 0
////	for i := range buffers {
////		records += buffers[i][c.workerID].records
////	}
////	c.logf("processing %d records", records)
////	return c.exec(reducer, reduceStdinHandler, reduceStdoutHandler, logStream)
////}
////
////func reduceStdinHandler(c context, w io.WriteCloser) error {
////	bufs := make([]*buffer, len(buffers))
////	for i := range buffers {
////		bufs[i] = buffers[i][c.workerID]
////	}
////	return intermediateReduceStream(c, w, bufs)
////}
////
////func reduceStdoutHandler(c context, r io.ReadCloser) error {
////	return outputStream(c, r, tempOutput)
////}
////
////
////// commit ensures transactional termination of succesfull jobs. If the job is configured to create
////// output commit uses a directory move to transactioanlly "commit" the output from a temporary
////// folder to the final output folder.
////func commit() {
////	if err := os.Rename(tempOutput, output); err != nil {
////		log.Printf("  error moving output data from %s to %s - %v", tempOutput, output, err)
////		log.Printf("  temporary data directory %s was not removed", tempDir)
////		log.Print("failed")
////		os.Exit(1)
////	}
////}
////
////// printOutput reads the output from the temporary directory and copies it to stdout. This is used
////// by runs without output to display the output in the terminal instead of writting it to a file.
////func printOutput() {
////	files, err := ioutil.ReadDir(tempOutput)
////	if err != nil {
////		log.Printf("  error reading output data in %s - %v", tempOutput, err)
////		os.Exit(1)
////	}
////	for _, file := range files {
////		filename := path.Join(tempOutput, file.Name())
////		f, err := os.Open(filename)
////		if err != nil {
////			log.Printf("  error reading output data in %s - %v", filename, err)
////			os.Exit(1)
////		}
////		r := bufio.NewReader(f)
////		w := bufio.NewWriter(os.Stdout)
////		if _, err := io.Copy(w, r); err != nil {
////			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
////		}
////		if err := w.Flush(); err != nil {
////			log.Printf("  error copying output data in %s to stdout - %v", filename, err)
////		}
////	}
////}
////
////func hasInput() bool {
////	return len(input) > 0
////}
////
////func hasMapper() bool {
////	return len(mapper) > 0
////}
////
////func hasReducer() bool {
////	return len(reducer) > 0
////}
////
////func hasOutput() bool {
////	return len(output) > 0
////}
