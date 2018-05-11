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
	si := readInt(b, i*32)
	spi := 16
	if si < spi {
		spi = si
	}
	sj := readInt(b, j*32)
	spj := 16
	if sj < spj {
		spj = sj
	}
	n := bytes.Compare(b[i*32+8:i*32+8+spi], b[j*32+8:j*32+8+spj])
	if n == 0 && (si > 16 || sj > 16) {
		si -= spi
		sj -= spj
		pi := readInt(b, i*32+24)
		pj := readInt(b, j*32+24)
		n = bytes.Compare(b[pi:pi+si], b[pj:pj+sj])
	}
	return n
}

func swap(buf []byte, i, j int) {
	tmp := make([]byte, 32)
	bi := buf[i*32 : i*32+32]
	bj := buf[j*32 : j*32+32]
	copy(tmp, bi)
	copy(bi, bj)
	copy(bj, tmp)
}
