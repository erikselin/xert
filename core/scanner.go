package core

import (
	"bufio"
	"fmt"
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

// next advances the scanner to the next record. The record is read from the read buffer together
// with bytes from the last record (since it is front compressed). This function is also
// responsible for growing the record buffer if it is too small to hold the next record.
func (s *fileScanner) next() bool {
	if s.e != nil {
		return false
	}
	s.lst, s.nxt = s.nxt, s.lst
	pn, err := readVarInt(s.r)
	if err != nil {
		if err != io.EOF {
			s.e = fmt.Errorf("error reading prefix length from file: %v", err)
		}
		if err := s.f.Close(); s.e == nil && err != nil {
			s.e = fmt.Errorf("error closing file: %v", err)
		}
		return false
	}
	rn, err := readVarInt(s.r)
	if err != nil {
		s.e = fmt.Errorf("error reading record length from file: %v", err)
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
			s.e = fmt.Errorf("error reading record from file: %v", err)
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

// readVarInt reads a variable length integer from a read buffer. This function will return a
// io.EOF iff the read of the first byte of the varint results in a io.EOF.
func readVarInt(r *bufio.Reader) (int, error) {
	n := 0
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			if i == 0 && err == io.EOF {
				return -1, err
			}
			return -1, fmt.Errorf("error reading varint byte: %v", err)
		}
		n |= int(b&0x7F) << uint(7*i)
		if b&0x80 == 0 {
			break
		}
	}
	return n, nil
}
