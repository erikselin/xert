package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

const (
	keyDelimiter    = '\t'
	recordDelimiter = '\n'
)

// streamClosed ...
func streamClosed(c *context, r io.ReadCloser) error {
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err == nil {
		return c.err("unexpected write to stdout for job without -output")
	}
	return nil
}

// streamToLog ...
func streamToLog(c *context, r io.ReadCloser) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		c.log(s.Text())
	}
	return s.Err()
}

// streamFromInput ...
func streamFromInput(
	c *context,
	w io.WriteCloser,
	splits chan *split,
) error {
	b := make([]byte, 1)

	for s := range splits {
		if s.err != nil {
			return c.err(s.err.Error())
		}

		c.logf("processing %s [%d:%d]", s.filename, s.start, s.end)

		f, err := os.Open(s.filename)
		if err != nil {
			return err
		}

		if _, err := f.Seek(s.start, 0); err != nil {
			return err
		}

		n := s.end - s.start
		if s.start > 0 {
			for {
				_, err := f.Read(b)
				n--

				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				if n == 0 {
					// TODO not sure a break is what we want here
					break
					//return errors.New("n <= 0")
				}

				if b[0] == recordDelimiter {
					break
				}
			}
		}

		if _, err := io.CopyN(w, f, n); err != nil {
			return err
		}

		for {
			_, err := f.Read(b)
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if _, err := w.Write(b); err != nil {
				return err
			}

			if b[0] == recordDelimiter {
				break
			}
		}
	}

	return w.Close()
}

// streamToOutput ...
func streamToOutput(
	c *context,
	r io.ReadCloser,
	output string,
) error {
	name := fmt.Sprintf("part-%d", c.workerID)
	path := path.Join(output, name)

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, r); err != nil {
		return err
	}

	return f.Close()
}

func streamToShuffleSort(
	c *context,
	r io.ReadCloser,
	buffers []*buffer,
) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		i, record, err := parse(s.Bytes())
		if err != nil {
			return err
		}
		if err := buffers[i].add(record); err != nil {
			return err
		}
	}
	return s.Err()
}

func parse(record []byte) (int, []byte, error) {
	stop := bytes.IndexByte(record, keyDelimiter)
	if stop == -1 {
		stop = len(record)
	}

	p, err := strconv.Atoi(string(record[0:stop]))
	if err != nil {
		return 0, []byte{}, err
	}

	if p < 0 || reducers <= p {
		return 0, []byte{}, fmt.Errorf("partition key was %d - needs to be in [0, %d)", p, reducers)
	}

	return p, record[stop+1 : len(record)], nil
}

func streamFromShuffleSort(
	c *context,
	w io.WriteCloser,
	buffers []*buffer,
) error {
	wb := bufio.NewWriter(w)

	scanners := make([]recordScanner, 0)
	for _, b := range buffers {
		mbuf, fbuf := newRecordScanners(b)
		scanners = append(scanners, mbuf)
		if fbuf != nil {
			scanners = append(scanners, fbuf)
		}
	}

	m, err := newMerger(scanners)
	if err != nil {
		return err
	}

	for m.next() {
		if _, err := wb.Write(m.record()); err != nil {
			return err
		}
		if err := wb.WriteByte(recordDelimiter); err != nil {
			return err
		}
	}

	if err := m.err(); err != nil {
		return err
	}

	if err := wb.Flush(); err != nil {
		return err
	}

	return w.Close()
}
