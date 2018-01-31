package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
)

// buffer ...
type buffer struct {
	head     int
	tail     int
	buf      []byte
	spills   int
	spillDir string
}

// Len ...
func (b *buffer) Len() int {
	return b.head / 8
}

// Swap ...
func (b *buffer) Swap(i, j int) {
	bi := b.buf[i*8 : i*8+8]
	bj := b.buf[j*8 : j*8+8]
	tmp := make([]byte, 8)
	copy(tmp, bi)
	copy(bi, bj)
	copy(bj, tmp)
}

// Less ...
func (b *buffer) Less(i, j int) bool {
	return bytes.Compare(b.readRecord(i), b.readRecord(j)) < 0
}

// add ...
func (b *buffer) add(record []byte) error {
	recordSize := 2*8 + len(record)

	if len(b.buf) < recordSize {
		return fmt.Errorf(
			"record is too large to fit in memory - required: %db but "+
				"buffer memory can only hold %db",
			recordSize,
			len(b.buf),
		)
	}

	if b.free() < recordSize {
		if err := b.spill(); err != nil {
			return err
		}
	}

	b.appendRecord(record)

	return nil
}

// sort ...
func (b *buffer) sort() {
	sort.Sort(b)
}

// spill ...
func (b *buffer) spill() error {
	defer func() {
		b.head = 0
		b.tail = len(b.buf)
		b.spills++
	}()

	sort.Sort(b)

	if err := os.MkdirAll(b.spillDir, 0700); err != nil {
		return err
	}

	filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", b.spills))
	w, err := os.Create(filename)

	if err != nil {
		return err
	}

	wb := bufio.NewWriter(w)

	for i := 0; i < b.Len(); i++ {
		p := b.readInt(i * 8)
		s := b.readInt(p)
		if _, err := wb.Write(b.buf[p : p+8+s]); err != nil {
			return err
		}
	}

	if err := wb.Flush(); err != nil {
		return err
	}

	return w.Close()
}

// extSort ...
func (b *buffer) externalSort() error {
	ways := mappers
	if ways < 2 {
		ways = 2
	}

	for b.spills > 1 {
		newSpills := 0
		for i := 0; i <= b.spills/ways; i++ {
			newSpills++
			start := i * ways
			end := start + ways
			if end >= b.spills {
				end = b.spills
			}

			scanners := make([]recordScanner, end-start)
			for j := 0; j < end-start; j++ {
				filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", j+start))
				scanners[j] = newFileScanner(filename)
			}

			m, err := newMerger(scanners)
			if err != nil {
				return err
			}

			mergeFilename := path.Join(b.spillDir, "merge")
			f, err := os.Create(mergeFilename)
			if err != nil {
				return err
			}

			w := bufio.NewWriter(f)
			buf := make([]byte, 8)
			for m.next() {
				r := m.record()
				n := len(r)
				buf[0] = byte(n)
				buf[1] = byte(n >> 8)
				buf[2] = byte(n >> 16)
				buf[3] = byte(n >> 24)
				buf[4] = byte(n >> 32)
				buf[5] = byte(n >> 40)
				buf[6] = byte(n >> 48)
				buf[7] = byte(n >> 56)
				if _, err := w.Write(buf); err != nil {
					return err
				}
				if _, err := w.Write(r); err != nil {
					return err
				}
			}

			if err := m.err(); err != nil {
				return err
			}

			if err := w.Flush(); err != nil {
				return err
			}

			if err := f.Close(); err != nil {
				return err
			}

			for j := 0; j < end-start; j++ {
				filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", j+start))
				if err := os.Remove(filename); err != nil {
					return err
				}
			}

			filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", i))
			if err := os.Rename(mergeFilename, filename); err != nil {
				return err
			}

		}
		b.spills = newSpills
	}

	return nil
}

// free ...
func (b *buffer) free() int {
	return b.tail - b.head
}

func (b *buffer) readInt(i int) int {
	return int(b.buf[i]) +
		int(b.buf[i+1])<<8 +
		int(b.buf[i+2])<<16 +
		int(b.buf[i+3])<<24 +
		int(b.buf[i+4])<<32 +
		int(b.buf[i+5])<<40 +
		int(b.buf[i+6])<<48 +
		int(b.buf[i+7])<<56
}

func (b *buffer) writeInt(i, n int) {
	b.buf[i] = byte(n)
	b.buf[i+1] = byte(n >> 8)
	b.buf[i+2] = byte(n >> 16)
	b.buf[i+3] = byte(n >> 24)
	b.buf[i+4] = byte(n >> 32)
	b.buf[i+5] = byte(n >> 40)
	b.buf[i+6] = byte(n >> 48)
	b.buf[i+7] = byte(n >> 56)
}

func (b *buffer) readRecord(i int) []byte {
	p := b.readInt(i * 8)
	s := b.readInt(p)
	return b.buf[p+8 : p+8+s]
}

func (b *buffer) appendRecord(record []byte) {
	b.tail -= len(record)
	copy(b.buf[b.tail:b.tail+len(record)], record)
	b.tail -= 8
	b.writeInt(b.tail, len(record))
	b.writeInt(b.head, b.tail)
	b.head += 8
}

// newBuffer ...
func newBuffer(mapperID, reducerID, bufMem int, tempSpill string) *buffer {
	return &buffer{
		head:   0,
		tail:   bufMem,
		buf:    make([]byte, bufMem),
		spills: 0,
		spillDir: path.Join(
			tempSpill,
			strconv.Itoa(mapperID),
			strconv.Itoa(reducerID),
		),
	}
}
