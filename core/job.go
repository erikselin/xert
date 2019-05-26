package core

import "io"

// Context ...
type Context struct {
	WorkerID int
	Mappers  int
	Reducers int
	Log      Logger
}

// MapperContext ...
type MapperContext struct {
	Context
	Input  io.Reader
	Output RecordWriter
}

// ReducerContext ...
type ReducerContext struct {
	Context
	Input  RecordReader
	Output io.Writer
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
	Input    string
	Mapper   MapperFunc
	Mappers  int
	Memory   string
	Output   string
	Reducer  ReducerFunc
	Reducers int
	TempDir  string
}

// Run ...
func Run(job Job) error {

}

func NewJob() (Job, error) {

}
