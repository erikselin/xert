package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
)

const recordDelimiter = '\n'

func logStream(c context, r io.ReadCloser) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		c.log(s.Text())
	}
	return s.Err()
}

func closedStream(c context, r io.ReadCloser) error {
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err == nil {
		return c.err("unexpected write to stdout for job without -output")
	}
	return nil
}

func inputStream(c context, w io.WriteCloser, inputChunks chan *chunk) error {
	for chunk := range inputChunks {
		if chunk.err != nil {
			return c.err(chunk.err.Error())
		}
		c.logf("processing %s [%d:%d]", chunk.filename, chunk.start, chunk.end)
		if err := chunk.writeTo(w); err != nil {
			return c.err(err.Error())
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

func intermediateReduceStream(c context, w io.WriteCloser, buffers []*buffer) error {
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
