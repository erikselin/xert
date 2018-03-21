package main

import (
	"fmt"
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

type chunk struct {
	filename string
	start    int64
	end      int64
	err      error
}

func (c *chunk) writeTo(w io.Writer) error { // TODO closer?
	buf := make([]byte, 1)
	f, err := os.Open(c.filename)
	if err != nil {
		return err
	}
	if _, err := f.Seek(c.start, 0); err != nil {
		return err
	}
	if c.start > 0 {
		for {
			c.start++
			_, err := f.Read(buf)
			if err == io.EOF {
				return f.Close()
			}
			if err != nil {
				return err
			}
			if c.start == c.end {
				return f.Close()
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
			return f.Close()
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
		if buf[0] == recordSeparator {
			return f.Close()
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

func extractRoot(input string) (string, error) {
	root := ""
	part := ""
	for _, c := range input {
		switch c {
		case '*', '?', '{', '[':
			return root, nil
		case '/':
			root = fmt.Sprintf("%s%s%c", root, part, c)
			part = ""
		default:
			part = fmt.Sprintf("%s%c", part, c)
		}
	}
	if root == "" {
		return os.Getwd()
	}
	return root, nil
}

func extractRegex(input string) (*regexp.Regexp, error) {
	regex := ""
	for _, c := range input {
		switch c {
		case '.', '$', '(', ')', '|', '+':
			regex = fmt.Sprintf("%s%c", regex, '\\')
		case '*':
			regex = fmt.Sprintf("%s%s", regex, "[^/]")
		case '?':
			regex = fmt.Sprintf("%s%c", regex, '.')
			continue
		case '{':
			regex = fmt.Sprintf("%s%s", regex, "(?:")
			continue
		case ',':
			regex = fmt.Sprintf("%s%c", regex, '|')
			continue
		case '}':
			regex = fmt.Sprintf("%s%c", regex, ')')
			continue
		}
		regex = fmt.Sprintf("%s%c", regex, c)
	}
	return regexp.Compile(regex)
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
