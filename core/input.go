package xrt

import (
	"io/ioutil"
	"os"
	"path"
	"regexp"
)

const (
	splitSize       int64 = 16 << 20 // 16mb
	recordSeparator byte  = '\n'
)

//type Input interface {
//	NewInputReader(int) io.Reader
//}
//
//func NewInput(readers int, input string) (Input, error) {
//
//}

// NewInputReader ...
//func (i *input) NewInputReader() io.Reader {
//	return inputReader{i.splits}
//}

// NewInput ...
//func NewInput(input string) Input {
//	splits := make(chan *split, conf.mappers)
//	go startWalk(conf.inputRoot, conf.inputRegex, splits)
//	return &input{splits}
//}

func startWalk(root string, regex *regexp.Regexp, splits chan *split) {
	if err := walk(root, regex, splits); err != nil {
		splits <- &split{"", -1, -1, err}
	}
	close(splits)
}

func walk(filename string, regex *regexp.Regexp, splits chan *split) error {
	s, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if s.Mode().IsRegular() && regex.Match([]byte(filename)) {
		start := int64(0)
		for start+splitSize < s.Size() {
			splits <- &split{filename, start, start + splitSize, nil}
			start += splitSize
		}
		splits <- &split{filename, start, s.Size(), nil}
		return nil
	}
	if s.Mode().IsDir() {
		fis, err := ioutil.ReadDir(filename)
		if err != nil {
			return err
		}
		for _, fi := range fis {
			if err := walk(path.Join(filename, fi.Name()), regex, splits); err != nil {
				return err
			}
		}
	}
	return nil
}

type split struct {
	filename string
	start    int64
	end      int64
	err      error
}

//func (s *split) writeTo(p []byte) (int, error) {
//	if pos == 0 {
//
//	}
//
//}
//
//func (c *chunk) copyChunk(f *os.File, w io.Writer) error {
//	buf := make([]byte, 1)
//	if _, err := f.Seek(c.start, 0); err != nil {
//		return err
//	}
//	if c.start > 0 {
//		for {
//			c.start++
//			_, err := f.Read(buf)
//			if err == io.EOF {
//				return nil
//			}
//			if err != nil {
//				return err
//			}
//			if c.start == c.end {
//				return nil
//			}
//			if buf[0] == recordSeparator {
//				break
//			}
//		}
//	}
//	if _, err := io.CopyN(w, f, c.end-c.start); err != nil {
//		return err
//	}
//	for {
//		_, err := f.Read(buf)
//		if err == io.EOF {
//			return nil
//		}
//		if err != nil {
//			return err
//		}
//		if _, err := w.Write(buf); err != nil {
//			return err
//		}
//		if buf[0] == recordSeparator {
//			return nil
//		}
//	}
//}

//type inputReader struct {
//	input    *input
//	pos      int
//	current  *split
//	previous *split
//	f        *os.File
//}
//
//func (i *inputReader) Read(b []byte) (int, error) {
//	n := 0
//	for n < len(b) {
//
//	}
//
//	var more bool
//	var err error
//	n := 0
//	for n < len(b) {
//		if i.current == nil || i.current.start >= i.current.end {
//			i.previous = i.current
//			i.current, more = i.input.next()
//			if !more {
//				return n, io.EOF
//			}
//			if i.previous != nil && i.previous.filename != i.current.filename {
//				if i.f, err = os.Open(i.current.filename); err != nil {
//					return -1, err
//				}
//			}
//			if _, err := i.f.Seek(i.current.start); err != nil {
//				return -1, err
//			}
//			if i.current.start > 0 {
//				buf := make([]byte, 1)
//				for {
//					i.current.start++
//					_, err := f.Read(buf)
//					if err == io.EOF {
//						return n, io.EOF
//					}
//					if err != nil {
//						return -1, err
//					}
//					if i.current.start == i.current.end {
//						// TODO get next split
//					}
//					if buf[0] == recordSeparator {
//						break
//					}
//				}
//			}
//		}
//		remaining := i.current.end - i.current.start
//		read := len(b) - n
//		if read > remaining {
//			read = remaining
//		}
//		m, err := i.f.Read(b[n:read])
//		if err != nil {
//			return -1, err
//		}
//		n += m
//	}
//	return n, nil
//}
