package main

import "bytes"

// merger joins the in-order streams of records from multiple bufferScanners
// into a single in-order stream of records.
type merger struct {
	nextRecord recordScanner
	nextError  error
	tail       int
	heap       []recordScanner
}

// next ...
func (m *merger) next() bool {
	if m.nextRecord != nil && m.nextRecord.next() {
		m.insert(m.nextRecord)
	}

	if m.tail < 0 {
		return false
	}

	m.nextRecord = m.root()

	if err := m.nextRecord.err(); err != nil {
		m.nextError = err
		return false
	}

	return true
}

// record ...
func (m *merger) record() []byte {
	return m.nextRecord.record()
}

// err ...
func (m *merger) err() error {
	return m.nextError
}

// insert ...
func (m *merger) insert(s recordScanner) {
	m.tail++

	i := m.tail
	for i > 0 && bytes.Compare(s.record(), m.heap[(i-1)/2].record()) < 0 {
		m.heap[i] = m.heap[(i-1)/2]
		i = (i - 1) / 2
	}

	m.heap[i] = s
}

// root ...
func (m *merger) root() recordScanner {
	root := m.heap[0]
	m.tail--

	if m.tail >= 0 {
		m.heap[0] = m.heap[m.tail+1]

		i := 0
		leftChild := 2*i + 1
		rightChild := 2*i + 2

		for leftChild <= m.tail {
			minChild := leftChild
			minRecord := m.heap[leftChild].record()

			if rightChild <= m.tail {
				rightRecord := m.heap[rightChild].record()
				if bytes.Compare(rightRecord, minRecord) < 0 {
					minChild = rightChild
					minRecord = m.heap[rightChild].record()
				}
			}

			if bytes.Compare(m.heap[i].record(), minRecord) <= 0 {
				break
			}

			tmp := m.heap[i]
			m.heap[i] = m.heap[minChild]
			m.heap[minChild] = tmp

			i = minChild
			leftChild = 2*i + 1
			rightChild = 2*i + 2
		}
	}

	return root
}

// newMerger ...
func newMerger(scanners []recordScanner) (*merger, error) {
	m := &merger{
		nextRecord: nil,
		tail:       -1,
		heap:       make([]recordScanner, len(scanners)),
	}

	for _, s := range scanners {
		if s.next() {
			m.insert(s)
		}

		if err := s.err(); err != nil {
			return nil, err
		}
	}

	return m, nil
}
