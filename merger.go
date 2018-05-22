package main

import "bytes"

// merger joins the in-order streams of records from multiple bufferScanners
// into a single in-order stream of records.
type merger struct {
	lst  scanner
	nxt  scanner
	e    error
	tail int
	heap []scanner
}

// next ...
func (m *merger) next() bool {
	m.lst = m.nxt
	if m.nxt != nil && m.nxt.next() {
		if bytes.Compare(m.nxt.lastRecord(), m.nxt.nextRecord()) == 0 {
			return true
		}
		m.push(m.nxt)
	}
	if m.tail < 0 {
		return false
	}
	m.nxt = m.pop()
	if err := m.nxt.err(); err != nil {
		m.e = err
		return false
	}
	return true
}

func (m *merger) nextRecord() []byte {
	return m.nxt.nextRecord()
}

func (m *merger) lastRecord() []byte {
	if m.lst == nil {
		return []byte{}
	}
	return m.lst.lastRecord()
}

// err ...
func (m *merger) err() error {
	return m.e
}

// push ...
func (m *merger) push(s scanner) {
	m.tail++
	i := m.tail
	for i > 0 && bytes.Compare(s.nextRecord(), m.heap[(i-1)/2].nextRecord()) < 0 {
		m.heap[i] = m.heap[(i-1)/2]
		i = (i - 1) / 2
	}
	m.heap[i] = s
}

// pop ...
func (m *merger) pop() scanner {
	root := m.heap[0]
	m.tail--
	if m.tail >= 0 {
		m.heap[0] = m.heap[m.tail+1]
		i := 0
		leftChild := 2*i + 1
		rightChild := 2*i + 2
		for leftChild <= m.tail {
			minChild := leftChild
			minRecord := m.heap[leftChild].nextRecord()
			if rightChild <= m.tail {
				rightRecord := m.heap[rightChild].nextRecord()
				if bytes.Compare(rightRecord, minRecord) < 0 {
					minChild = rightChild
					minRecord = m.heap[rightChild].nextRecord()
				}
			}
			if bytes.Compare(m.heap[i].nextRecord(), minRecord) <= 0 {
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
func newMerger(scanners []scanner) (*merger, error) {
	m := &merger{
		tail: -1,
		heap: make([]scanner, len(scanners)),
	}
	for _, s := range scanners {
		if s.next() {
			m.push(s)
		}
		if err := s.err(); err != nil {
			return nil, err
		}
	}
	return m, nil
}
