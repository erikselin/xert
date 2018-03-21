// +build gccgo !amd64

package main

import "bytes"

func readInt(b []byte, i int) int {
	return int(b[i]) +
		int(b[i+1])<<8 +
		int(b[i+2])<<16 +
		int(b[i+3])<<24 +
		int(b[i+4])<<32 +
		int(b[i+5])<<40 +
		int(b[i+6])<<48 +
		int(b[i+7])<<56
}

func writeInt(b []byte, i, n int) {
	b[i] = byte(n)
	b[i+1] = byte(n >> 8)
	b[i+2] = byte(n >> 16)
	b[i+3] = byte(n >> 24)
	b[i+4] = byte(n >> 32)
	b[i+5] = byte(n >> 40)
	b[i+6] = byte(n >> 48)
	b[i+7] = byte(n >> 56)
}

func compare(b []byte, i, j int) int {
	pi := readInt(b, i*8)
	si := readInt(b, pi)
	pj := readInt(b, j*8)
	sj := readInt(b, pj)
	return bytes.Compare(b[pi+8:pi+8+si], b[pj+8:pj+8+sj])
}

func swap(buf []byte, i, j int) {
	tmp := make([]byte, 8)
	bi := buf[i*8 : i*8+8]
	bj := buf[j*8 : j*8+8]
	copy(tmp, bi)
	copy(bi, bj)
	copy(bj, tmp)
}
