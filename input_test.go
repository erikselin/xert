package main

import "testing"

var extractTests = []struct {
	in       string
	outRoot  string
	outRegex string
}{
	{"foo", "", "foo"},
	{"foo/", "foo/", "foo/"},
	{"foo/?", "foo/", "foo/."},
	{"foo/ba.*", "foo/", "foo/ba\\.[^/]*"},
	{"foo/[ab]/bar", "foo/", "foo/[ab]/bar"},
	{"/foo/[^ab]/bar", "/foo/", "/foo/[^ab]/bar"},
	{"/foo/[a-b]/bar", "/foo/", "/foo/[a-b]/bar"},
	{"/foo/[^a-b]/bar", "/foo/", "/foo/[^a-b]/bar"},
	{"/foo/{a,b}/bar", "/foo/", "/foo/(?:a|b)/bar"},
	{"/foo/b+r.biz", "/foo/", "/foo/b\\+r\\.biz"},
}

func TestExtractRoot(t *testing.T) {
	for _, tt := range extractTests {
		out := extractRoot(tt.in)
		if out != tt.outRoot {
			t.Errorf("extractRoot(%s) => '%s', want '%s'", tt.in, out, tt.outRoot)
			continue
		}
	}
}

func TestExtractRegex(t *testing.T) {
	for _, tt := range extractTests {
		out := extractRegex(tt.in)
		if out != tt.outRegex {
			t.Errorf("extractRegex(%s) => '%s', want '%s'", tt.in, out, tt.outRegex)
			continue
		}
	}
}
