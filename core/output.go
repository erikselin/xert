package xrt

type output interface {
}

func (out *output) newOutputWriter(workerID int) *outputWriter {

}

func newOutput(output string) *output {

}

type outputWriter struct {
	workerID int
}

func (w *outputWriter) Write(p []byte) (int, error) {

}
