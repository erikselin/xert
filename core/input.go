package core

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
)

const (
	chunkSize       int64 = 16 << 20 // 16mb
	recordSeparator byte  = '\n'
)

type input struct {
}

func (in *input) newWorkerInput(conf WorkerConfig) workerInput {
}

func newInput(input string) *input {
}

type workerInput struct {
	splits chan *split
	split  *split
	pos    int
	f      *os.File
}

// Read ...
func (wi *workerInput) Read(p []byte) (int, error) {
	j := 0
	for j < len(p) {
		if i.split == nil {
			return j, io.EOF
		}
		n, err := i.split.writeTo(p[j:])
		j += n
		if err != nil {
			if err == io.EOF {
				i.nextSplit()
			} else {
				return -1, err
			}
		}
	}
	return j, nil
}

func newInput(splits chan *split) input {
	i := input{
		splits: splits,
	}
	i.nextSplit()
	return i
}

type split struct {
	filename string
	start    int64
	end      int64
	err      error
}

func (s *split) writeTo(p []byte) (int, error) {
	if pos == 0 {

	}

}

func (c *chunk) copyChunk(f *os.File, w io.Writer) error {
	buf := make([]byte, 1)
	if _, err := f.Seek(c.start, 0); err != nil {
		return err
	}
	if c.start > 0 {
		for {
			c.start++
			_, err := f.Read(buf)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if c.start == c.end {
				return nil
			}
			if buf[0] == recordSeparator {
				break
			}
		}
	}
	if _, err := io.CopyN(w, f, c.end-c.start); err != nil {
		return err
	}
	for {
		_, err := f.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
		if buf[0] == recordSeparator {
			return nil
		}
	}
}

func enumerateChunks(input string) (chan *chunk, error) {
	abs, err := filepath.Abs(input)
	if err != nil {
		return nil, err
	}
	regex, err := extractRegex(abs)
	if err != nil {
		return nil, err
	}
	root, err := extractRoot(abs)
	if err != nil {
		return nil, err
	}
	chunks := make(chan *chunk)
	go startWalk(root, regex, chunks)
	return chunks, nil
}

func startWalk(root string, regex *regexp.Regexp, chunks chan *chunk) {
	if err := walk(root, regex, chunks); err != nil {
		chunks <- &chunk{"", -1, -1, err}
	}
	close(chunks)
}

func walk(filename string, regex *regexp.Regexp, chunks chan *chunk) error {
	s, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if s.Mode().IsRegular() && regex.Match([]byte(filename)) {
		start := int64(0)
		for start+chunkSize < s.Size() {
			chunks <- &chunk{filename, start, start + chunkSize, nil}
			start += chunkSize
		}
		chunks <- &chunk{filename, start, s.Size(), nil}
		return nil
	}
	if s.Mode().IsDir() {
		fis, err := ioutil.ReadDir(filename)
		if err != nil {
			return err
		}
		for _, fi := range fis {
			if err := walk(path.Join(filename, fi.Name()), regex, chunks); err != nil {
				return err
			}
		}
	}
	return nil
}
