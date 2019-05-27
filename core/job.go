// Package xrt/core provides the xrt runtime together with a simple Go API making it possible to
// implement xrt tasks in Go which are compiled into a single binary and executed without any
// cross-process communication. The xrt/core Go API is what the main xrt tool is using internally
// and can be used as a optimization if a regular xrt task is becoming too slow.
//
// Example:
//
//    import (
//        xrt "github.com/erikselin/xrt/core"
//    )
//
package core

import (
	"io"
	"os"
)

func main() {
	if err := xrt.
		NewJob(4, mapper).
		WithInput("data.csv").
		WithReducer(4, reducer).
		WithOutput("results.csv").
		Run(); err != nil {
		os.Exit(1)
	}
}

// MapperContext ...
type MapperContext struct {
	WorkerID int
	Mappers  int
	Reducers int
	Log      Logger
	Input    io.Reader
	Output   RecordWriter
}

// ReducerContext ...
type ReducerContext struct {
	WorkerID int
	Mappers  int
	Reducers int
	Log      Logger
	Input    RecordReader
	Output   io.Writer
}

// MapperFunc ...
type MapperFunc func(MapperContext) error

// ReducerFunc ...
type ReducerFunc func(ReducerContext) error

// RecordWriter ...
type RecordWriter interface {
	WriteRecord(int, []byte) error
}

// RecordReader ...
type RecordReader interface {
	Next() bool
	Record() []byte
	Err() error
}

// Job ...
type Job struct {
	input    string
	mapper   MapperFunc
	mappers  int
	memory   string
	output   string
	reducer  ReducerFunc
	reducers int
	tempDir  string
}

func (j Job) validate() error {

}

func (j Job) run() error {

}

// Run ...
func (j Job) Run() error {
	if err := j.validate(); err != nil {
		return err
	}
	return j.run()
}

// WithInput ...
func (j Job) WithInput(input string) Job {
	return Job{input, j.mapper, j.mappers, j.memory, j.output, j.reducer, j.reducers, j.tempDir}
}

// WithMemory ...
func (j Job) WithMemory(memory string) Job {
	return Job{j.input, j.mapper, j.mappers, memory, j.output, j.reducer, j.reducers, j.tempDir}
}

// WithOutput ...
func (j Job) WithOutput(output string) Job {
	return Job{j.input, j.mapper, j.mappers, j.memory, output, j.reducer, j.reducers, j.tempDir}
}

// WithReduce ...
func (j Job) WithReduce(reducers int, reducer ReducerFunc) Job {
	return Job{j.input, j.mapper, j.mappers, j.memory, j.output, reducer, reducers, j.tempDir}
}

// WithTempDir ...
func (j Job) WithTempDir(tempDir string) Job {
	return Job{j.input, j.mapper, j.mappers, j.memory, j.output, j.reducer, j.reducers, tempDir}
}

// NewJob ...
func NewJob(mappers int, mapper MapperFunc) Job {
	return Job{j.input, mapper, mappers, j.memory, j.output, j.reducer, j.reducers, j.tempDir}
}
