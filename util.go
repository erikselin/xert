package main

import (
	"bytes"
	"fmt"
	"strconv"
)

const keyDelimiter = '\t'

// parseMemory takes a string representing a memory amount and converts it into
// a integer representign the number of bytes.
// For example:
//    parseMemory("1k") = 1024
//    parseMemory("1k") = 1048576
// -1 is returned if a bad memory string was provided.
func parseMemory(v string) int {
	var m uint
	switch v[len(v)-1] {
	case 'b':
		m = 0
	case 'k':
		m = 10
	case 'm':
		m = 20
	case 'g':
		m = 30
	case 't':
		m = 40
	case 'p':
		m = 50
	default:
		return -1
	}
	n, err := strconv.Atoi(v[0 : len(v)-1])
	if err != nil {
		return -1
	}
	return n << m
}

func parse(record []byte) (int, []byte, error) {
	stop := bytes.IndexByte(record, keyDelimiter)
	if stop == -1 {
		stop = len(record)
	}
	p, err := strconv.Atoi(string(record[0:stop]))
	if err != nil {
		return 0, []byte{}, err
	}
	if p < 0 || reducers <= p {
		return 0, []byte{}, fmt.Errorf("partition key was %d - needs to be in [0, %d)", p, reducers)
	}
	return p, record[stop+1 : len(record)], nil
}
