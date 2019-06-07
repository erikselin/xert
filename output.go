package xrt

import "io"

type Output interface {
	NewOutputWriter(int) io.WriteCloser
}

func NewOutput(writers int, output string) (Output, error) {

}
