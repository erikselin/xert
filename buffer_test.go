package main

import "testing"

func TestCompare(t *testing.T) {
	b := newBuffer(1000, ".")

	b.appendRecord([]byte(""))
	b.appendRecord([]byte("a"))
	b.appendRecord([]byte("b"))
	b.appendRecord([]byte("ab"))
	b.appendRecord([]byte("ba"))
	b.appendRecord([]byte("aaaaaaaaaaaaaaaaaa"))
	b.appendRecord([]byte("aaaaaaaabbbbbbbbbb"))
	b.appendRecord([]byte("bbbbbbbbaaaaaaaaaa"))
	b.appendRecord([]byte("bbbbbbbbbbbbbbbbbb"))

	for _, test := range []struct {
		ri  int
		rj  int
		out int
	}{
		{0, 0, 0},
		{1, 1, 0},
		{5, 5, 0},
		{0, 1, -1},
		{1, 0, 1},
		{1, 2, -1},
		{2, 1, 1},
		{1, 3, -1},
		{3, 1, 1},
		{3, 4, -1},
		{4, 3, 1},
		{1, 5, -1},
		{5, 1, 1},
		{5, 6, -1},
		{7, 5, 1},
	} {
		actual := compare(b.buf, test.ri, test.rj)
		if test.out != actual {
			t.Errorf("compare(b.buf, %d, %d) returned %d, want %d", test.ri, test.rj, actual, test.out)
		}
	}
}
