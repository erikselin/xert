package xrt

type output interface {
}

func (out *output) Write(b []byte) (int, error) {

}

func newOutput(worker int, output string) *output {

}
