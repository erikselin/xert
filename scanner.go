package main

import (
	"bufio"
	"io"
	"os"
)

// scanner provides the interface for the scanners capable of traversing
// the memory porting and disk portion of buffers.
type scanner interface {
	next() bool
	lastRecord() []byte
	nextRecord() []byte
	err() error
}

type memoryScanner struct {
	index int
	buf   *buffer
	lst   []byte
	nxt   []byte
}

func (s *memoryScanner) next() bool {
	s.lst, s.nxt = s.nxt, s.lst
	s.index++
	if s.index < s.buf.Len() {
		n := readInt(s.buf.buf, s.index*32)
		if n > cap(s.nxt) {
			s.nxt = make([]byte, 4096*(n%4096)+4096)
		}
		s.nxt = s.nxt[:n]
		pn := 16
		if n < pn {
			pn = n
		}
		copy(s.nxt[0:pn], s.buf.buf[s.index*32+8:s.index*32+8+pn])
		if n > 16 {
			p := readInt(s.buf.buf, s.index*32+24)
			copy(s.nxt[16:n], s.buf.buf[p:p+n-16])
		}
		return true
	}
	return false
}

func (s *memoryScanner) nextRecord() []byte {
	return s.nxt
}

func (s *memoryScanner) lastRecord() []byte {
	return s.lst
}

func (s *memoryScanner) err() error {
	return nil
}

func newMemoryScanner(b *buffer) *memoryScanner {
	return &memoryScanner{
		index: -1,
		buf:   b,
	}
}

type fileScanner struct {
	f   *os.File
	r   *bufio.Reader
	e   error
	lst []byte
	nxt []byte
}

func (s *fileScanner) next() bool {
	s.lst, s.nxt = s.nxt, s.lst
	if _, err := s.r.Peek(1); err != nil {
		if err != io.EOF {
			s.e = err
		}
		return false
	}
	pn, err := readVarInt(s.r)
	if err != nil {
		s.e = err
		return false
	}
	rn, err := readVarInt(s.r)
	if err != nil {
		s.e = err
		return false
	}
	n := pn + rn
	if n > cap(s.nxt) {
		s.nxt = make([]byte, 4096*(n/4096)+4096)
	}
	s.nxt = s.nxt[:n]
	copy(s.nxt[0:pn], s.lst[0:pn])
	for i := pn; i < n; {
		m, err := s.r.Read(s.nxt[i:n])
		if err != nil {
			s.e = err
			return false
		}
		i += m
	}
	return true
}

func (s *fileScanner) nextRecord() []byte {
	return s.nxt
}

func (s *fileScanner) lastRecord() []byte {
	return s.lst
}

func (s *fileScanner) err() error {
	return s.e
}

func newFileScanner(filename string) *fileScanner {
	s := &fileScanner{}
	s.f, s.e = os.Open(filename)
	if s.e != nil {
		return s
	}
	s.r = bufio.NewReader(s.f)
	return s
}

func readVarInt(r *bufio.Reader) (int, error) {
	n := 0
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return -1, err
		}
		n |= int(b&0x7F) << uint(7*i)
		if b&0x80 == 0 {
			break
		}
	}
	return n, nil
}
