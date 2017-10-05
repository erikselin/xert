package main

import (
	"io/ioutil"
	"os"
	"path"
)

const splitSize int64 = 64 << 20 // 10mb

// split ...
type split struct {
	filename string
	start    int64
	end      int64
	err      error
}

// enumerate ...
func enumerate(input string) chan *split {
	// TODO hadoop/linux like pattern matching support - for now assume input
	// is a directory.
	c := make(chan *split)

	go func() {
		if err := walk(c, input); err != nil {
			c <- &split{
				err: err,
			}
		}
		close(c)
	}()

	return c
}

func walk(c chan *split, filename string) error {
	s, err := os.Stat(filename)
	if err != nil {
		return err
	}

	if s.Mode().IsRegular() {
		s, err := os.Stat(filename)
		if err != nil {
			return err
		}

		var start int64
		for start+splitSize < s.Size() {
			c <- &split{
				filename: filename,
				start:    start,
				end:      start + splitSize,
			}
			start += splitSize
		}

		c <- &split{
			filename: filename,
			start:    start,
			end:      s.Size(),
		}

		return nil
	}

	fis, err := ioutil.ReadDir(filename)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if err := walk(c, path.Join(filename, fi.Name())); err != nil {
			return err
		}
	}

	return nil
}
