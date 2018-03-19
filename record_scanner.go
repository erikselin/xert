package main

import (
	"bufio"
	"io"
	"os"
	"path"
)

// recordScanner provides the interface for the scanners capable of traversing
// the memory porting and disk portion of buffers.
type recordScanner interface {
	next() bool
	record() []byte
	err() error
}

// memoryScanner provides a scanner for the in-memory part of a buffer.
// The buffer is traversed from left to right and records will be returned in
// order as long as the in-memory part of the buffer has already been sorted.
type memoryScanner struct {
	index      int
	buf        *buffer
	nextRecord []byte
}

func (s *memoryScanner) next() bool {
	s.index++
	more := s.index < s.buf.Len()
	if more {
		s.nextRecord = s.buf.readRecord(s.index)
	}
	return more
}

func (s *memoryScanner) record() []byte {
	return s.nextRecord
}

func (s *memoryScanner) err() error {
	return nil
}

// fileScanner provides a scanner for the on-disk part of a buffer. Files are
// read sequentially and if multiple files are present they will be read from
// lowest file index to highest file index. As long as the files have been
// externally sorted the fileScanner will return records in order.
type fileScanner struct {
	buf        []byte
	recordSize int
	f          *os.File
	r          *bufio.Reader
	e          error
}

func (s *fileScanner) next() bool {
	if s.f == nil || s.e != nil {
		return false
	}
	n := 0
	for n < 8 {
		m, err := s.r.Read(s.buf[n:8])
		if err != nil {
			if err != io.EOF {
				s.e = err
			}
			return false
		}
		n += m
	}
	s.recordSize = int(s.buf[0]) +
		int(s.buf[1])<<8 +
		int(s.buf[2])<<16 +
		int(s.buf[3])<<24 +
		int(s.buf[4])<<32 +
		int(s.buf[5])<<40 +
		int(s.buf[6])<<48 +
		int(s.buf[7])<<56
	if len(s.buf) < s.recordSize {
		s.buf = make([]byte, 4096*(s.recordSize/4096)+4096)
	}
	n = 0
	for n < s.recordSize {
		m, err := s.r.Read(s.buf[n:s.recordSize])
		if err != nil {
			s.e = err
			return false
		}
		n += m
	}
	return true
}

func (s *fileScanner) record() []byte {
	return s.buf[0:s.recordSize]
}

func (s *fileScanner) err() error {
	return s.e
}

func newFileScanner(filename string) *fileScanner {
	fbuf := &fileScanner{}
	f, err := os.Open(filename)
	r := bufio.NewReader(f)
	fbuf.f = f
	fbuf.r = r
	fbuf.e = err
	fbuf.buf = make([]byte, 4096)
	return fbuf
}

// newRecordScanners returns a memoryScanner and a fileScanner for the buffer.
func newRecordScanners(buf *buffer) (*memoryScanner, *fileScanner) {
	mbuf := &memoryScanner{
		index: -1,
		buf:   buf,
	}
	var fbuf *fileScanner
	if buf.spills > 0 {
		fbuf = newFileScanner(path.Join(buf.spillDir, "spill-0"))
	}
	return mbuf, fbuf
}
