package xrt

import (
	"io"
	"path"
)

type buffers struct {
	data [][]*buffer
}

func (b *buffers) newBufferReader(workerID int) (*bufferReader, error) {
	scanners := make([]scanner, 0)
	for _, d := range b.data {
		scanners = append(scanners, newMemoryScanner(d[workerID]))
		if d.spills > 0 {
			filename := path.Join(conf.tempSpill, "spill-0")
			scanners = append(scanners, newFileScanner(filename))
		}
	}
	m, err := newMerger(scanners)
	if err != nil {
		return nil, err
	}
	return &bufferReader{merger: m}, nil
}

func (b *buffer) newBufferWriter(workerID) *bufferWriter {
	return &bufferWriter{
		data: b.data[workerID],
	}
}

type bufferReader struct {
	recordsRead int // TODO do we really need this
	ptr         int
	current     []byte
	merger      *merger
}

func (r *bufferReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if r.ptr >= len(r.current) {
			if r.recordsRead > 0 {
				p[n] = recordDelimiter
				n++
			}
			if !r.merger.next() {
				if err := r.merger.err(); err != nil {
					return n, err
				}
				return n, io.EOF
			}
			r.current = r.merger.nextRecord()
			r.ptr = 0
			r.recordsRead++
		}
		m := len(r.current) - r.ptr
		if m > len(p)-n {
			m = len(p) - n
		}
		copy(p[n:], record[r.ptr:r.ptr+m])
		n += m
		r.ptr += m
	}
	return n, nil
}

type bufferWriter struct {
	data   []*buffer
	key    int
	record []byte
}

func (w *bufferWriter) Write(p []byte) (int, error) {
	// TODO go through the entire b slice writing any records
	// to the buffer and storing any remaining bytes for the next Write
	ptr := 0
	for ptr < len(p) {
		n := 0
		for {
			b := p[ptr]
			if b == tab { //todo this might not be correct syntax
				// skip delimiter,
				// done parsing number
				ptr++
				break
			}
			if b < 48 || 57 < b {
				// err cannot parse int
			}
			// continue parsing int
			ptr++
		}
		start := ptr
		end := ptr
		for {
			b := p[ptr]
			if b == recordDelimiter { //todo this might not be correct syntax
				// done parsing record
				end = ptr
				// skip delimiter,
				ptr++
				break
			}
			ptr++
		}
		if err := w.data[n].add(p[start:end]); err != nil {
			return err
		}
	}
}

func (w *bufferWriter) close() error {
	// process remaining bytes?
}

//// Buffer ...
//type Buffer interface {
//	NewBufferWriter(int) (BufferWriter, error)
//	NewBufferReader(int) (BufferReader, error)
//}
//
//// BufferReader ...
//type BufferReader interface {
//
//	// Next ...
//	Next() bool
//
//	// Record ...
//	Record() []byte
//
//	// Err ...
//	Err() error
//}
//
//// BufferWriter ...
//type BufferWriter interface {
//
//	// Write ...
//	Write(int, []byte) error
//}
//
//// NewBuffer ...
//func NewBuffer(memory, spillDir string, writers, readers int) Buffer {
//
//}

//import (
//	"bufio"
//	"fmt"
//	"os"
//	"path"
//	"sort"
//)

//type buffers [][]buffer

//
//// buffer ...
//type buffer struct {
//	head     int
//	tail     int
//	buf      []byte
//	records  int
//	spills   int
//	spillDir string
//}
//
//// Len ...
//func (b *buffer) Len() int {
//	return b.head / 32
//}
//
//// Swap ...
//func (b *buffer) Swap(i, j int) {
//	swap(b.buf, i, j)
//}
//
//// Less ...
//func (b *buffer) Less(i, j int) bool {
//	return compare(b.buf, i, j) < 0
//}
//
//// add ...
//func (b *buffer) add(record []byte) error {
//	recordSize := 16 + len(record)
//	if recordSize < 32 {
//		recordSize = 32
//	}
//	if b.free() < recordSize {
//		if len(b.buf) < recordSize {
//			return fmt.Errorf(
//				"record is too large to fit in memory - required: %db but "+
//					"buffer memory can only hold %db",
//				recordSize,
//				len(b.buf),
//			)
//		}
//		if err := b.spill(); err != nil {
//			return err
//		}
//	}
//	b.appendRecord(record)
//	b.records++
//	return nil
//}
//
//// sort ...
//func (b *buffer) sort() {
//	sort.Sort(b)
//}
//
//// spill ...
//func (b *buffer) spill() error {
//	defer func() {
//		b.head = 0
//		b.tail = len(b.buf)
//		b.spills++
//	}()
//	b.sort()
//	if err := os.MkdirAll(b.spillDir, 0700); err != nil {
//		return err
//	}
//	filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", b.spills))
//	w, err := os.Create(filename)
//	if err != nil {
//		return err
//	}
//	wb := bufio.NewWriter(w)
//	s := newMemoryScanner(b)
//	for s.next() {
//		if err := writeRecord(wb, s.lastRecord(), s.nextRecord()); err != nil {
//			return err
//		}
//	}
//	if err := s.err(); err != nil {
//		return err
//	}
//	if err := wb.Flush(); err != nil {
//		return err
//	}
//	return w.Close()
//}
//
//func (b *buffer) needExternalSort() bool {
//	return b.spills > 1
//}
//
//// extSort ...
//func (b *buffer) externalSort() error {
//	// During the final merge phase we will have at most mappers*reducers open files
//	// so use this here as well. With a hard minimum of 16 for any situation where we
//	// have < 16 mappers.
//	ways := mappers
//	if ways < 16 {
//		ways = 16
//	}
//	for b.needExternalSort() {
//		newSpills := 0
//		for i := 0; i <= b.spills/ways; i++ {
//			start := i * ways
//			end := start + ways
//			if end >= b.spills {
//				end = b.spills
//			}
//			if end-start == 0 {
//				continue
//			}
//			newSpills++
//			scanners := make([]scanner, end-start)
//			for j := 0; j < end-start; j++ {
//				filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", j+start))
//				scanners[j] = newFileScanner(filename)
//			}
//			m, err := newMerger(scanners)
//			if err != nil {
//				return err
//			}
//			mergeFilename := path.Join(b.spillDir, "merge")
//			f, err := os.Create(mergeFilename)
//			if err != nil {
//				return err
//			}
//			wb := bufio.NewWriter(f)
//			for m.next() {
//				if err := writeRecord(wb, m.lastRecord(), m.nextRecord()); err != nil {
//					return err
//				}
//			}
//			if err := m.err(); err != nil {
//				return err
//			}
//			if err := wb.Flush(); err != nil {
//				return err
//			}
//			if err := f.Close(); err != nil {
//				return err
//			}
//			for j := 0; j < end-start; j++ {
//				filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", j+start))
//				if err := os.Remove(filename); err != nil {
//					return err
//				}
//			}
//			filename := path.Join(b.spillDir, fmt.Sprintf("spill-%d", i))
//			if err := os.Rename(mergeFilename, filename); err != nil {
//				return err
//			}
//		}
//		b.spills = newSpills
//	}
//	return nil
//}
//
//// free ...
//func (b *buffer) free() int {
//	return b.tail - b.head
//}
//
//func (b *buffer) appendRecord(record []byte) {
//	writeInt(b.buf, b.head, len(record))
//	b.head += 8
//	n := 16
//	if len(record) < 16 {
//		n = len(record)
//	}
//	copy(b.buf[b.head:b.head+n], record[0:n])
//	b.head += 16
//	if len(record) > 16 {
//		b.tail = b.tail - len(record) + 16
//		copy(b.buf[b.tail:b.tail+len(record)-16], record[16:len(record)])
//		writeInt(b.buf, b.head, b.tail)
//	}
//	b.head += 8
//}
//
//// newBuffer ...
//func newBuffer(bufMem int, spillDir string) *buffer {
//	return &buffer{
//		head:     0,
//		tail:     bufMem,
//		buf:      make([]byte, bufMem),
//		records:  0,
//		spills:   0,
//		spillDir: spillDir,
//	}
//}
//
//func writeRecord(w *bufio.Writer, lst, nxt []byte) error {
//	m := len(lst)
//	if len(nxt) < m {
//		m = len(nxt)
//	}
//	pn := 0
//	for i := 0; i < m; i++ {
//		if lst[i] != nxt[i] {
//			break
//		}
//		pn++
//	}
//	if err := writeVarInt(w, pn); err != nil {
//		return err
//	}
//	if err := writeVarInt(w, len(nxt)-pn); err != nil {
//		return err
//	}
//	_, err := w.Write(nxt[pn:len(nxt)])
//	return err
//}
//
//func writeVarInt(w *bufio.Writer, n int) error {
//	for n >= 0x80 {
//		if err := w.WriteByte(byte(n) | 0x80); err != nil {
//			return err
//		}
//		n >>= 7
//	}
//	return w.WriteByte(byte(n))
//}
