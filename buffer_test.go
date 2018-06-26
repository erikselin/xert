package main

import (
	"bytes"
	"testing"
)

const testBufferSize = 1000000 // 1mb

func generateRecords(prefixSize int) [][]byte {
	records := [][]byte{}
	for i := 0; i < 9; i++ {
		record := make([]byte, prefixSize+i)
		for j := 0; j < len(record); j++ {
			if j < prefixSize {
				record[j] = 'x'
				continue
			}
			record[j] = 'a'
		}
		for done := false; !done; {
			records = append(records, append([]byte{}, record...))
			done = true
			for j := prefixSize; j < len(record); j++ {
				if record[j] == 'a' {
					done = false
					record[j] = 'b'
					break
				}
				record[j] = 'a'
			}
		}
	}
	return records
}

func TestCompare(t *testing.T) {
	b := newBuffer(testBufferSize, ".")
	records := generateRecords(0)                      // 0-8 bytes
	records = append(records, generateRecords(12)...)  // 12-20 bytes
	records = append(records, generateRecords(28)...)  // 28-36 bytes
	records = append(records, generateRecords(500)...) // 500+ bytes
	for _, record := range records {
		b.appendRecord(record)
	}
	for i := range records {
		for j := range records {
			expected := bytes.Compare(records[i], records[j])
			actual := compare(b.buf, i, j)
			if expected != actual {
				t.Errorf("compare(b.buf, %d=(%s), %d=(%s)) returned %d, want %d", i, records[i], j, records[j], actual, expected)
			}
		}
	}
}
