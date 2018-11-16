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
	recordDelimiter = '\n'
	keyDelimiter    = '\t'
)

func logStream(c context, r io.ReadCloser) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		c.log(s.Text())
	}
	return s.Err()
}

func inputStream(c context, w io.WriteCloser, inputChunks chan *chunk) error {
	var f *os.File
	var err error
	for chunk := range inputChunks {
		if chunk.err != nil {
			return c.err(chunk.err.Error())
		}
		if f == nil || f.Name() != chunk.filename {
			if f != nil {
				if err = f.Close(); err != nil {
					return err
				}
			}
			f, err = os.Open(chunk.filename)
			if err != nil {
				return err
			}
		}
		c.logf("processing %s [%d:%d]", chunk.filename, chunk.start, chunk.end)
		if err := chunk.copyChunk(f, w); err != nil {
			return c.err(err.Error())
		}
	}
	if f != nil {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return w.Close()
}

func outputStream(c context, r io.ReadCloser, output string) error {
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

func intermediateMapStream(c context, r io.ReadCloser, buffers []*buffer) error {
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

func intermediateReduceStream(c context, w io.WriteCloser, buffers []*buffer) error {
	wb := bufio.NewWriter(w)
	scanners := make([]scanner, 0)
	for _, b := range buffers {
		scanners = append(scanners, newMemoryScanner(b))
		if b.spills > 0 {
			filename := path.Join(b.spillDir, "spill-0")
			scanners = append(scanners, newFileScanner(filename))
		}
	}
	m, err := newMerger(scanners)
	if err != nil {
		return err
	}
	for m.next() {
		if _, err := wb.Write(m.nextRecord()); err != nil {
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
