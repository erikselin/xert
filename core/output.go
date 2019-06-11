package xrt

import (
	"fmt"
	"os"
	"path"
)

type output struct {
	dir string
}

func (out *output) newOutputWriter(workerID int) (*outputWriter, error) {
	name := fmt.Sprintf("%0.5d.part", c.workerID)
	path := path.Join(out.dir, name)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return outputWriter{f}, nil
}

func newOutput(dir string) *output {
	return &output{dir}
}

type outputWriter struct {
	f *os.File
}

func (w *outputWriter) Write(p []byte) (int, error) {
	// TODO we should keep track of bytes written and create new files when
	// needed to avoid very large output files. See if this logic can leverage
	// the same helper as we will/are using during spill merge.
	return f.Write(p)
}

func (w *outputWriter) done() error {
	return w.f.Close()
}
