package core

import (
	"os"
	"testing"
)

var wd, _ = os.Getwd()

var extractTests = []struct {
	in       string
	outRoot  string
	outRegex string
}{
	{"foo", wd, "^foo$"},
	{"foo/", "foo/", "^foo/$"},
	{"foo/?", "foo/", "^foo/.$"},
	{"foo/ba.*", "foo/", "^foo/ba\\.[^/]*$"},
	{"foo/[ab]/bar", "foo/", "^foo/[ab]/bar$"},
	{"/foo/[^ab]/bar", "/foo/", "^/foo/[^ab]/bar$"},
	{"/foo/[a-b]/bar", "/foo/", "^/foo/[a-b]/bar$"},
	{"/foo/[^a-b]/bar", "/foo/", "^/foo/[^a-b]/bar$"},
	{"/foo/{a,b}/bar", "/foo/", "^/foo/(?:a|b)/bar$"},
	{"/foo/b+r.biz", "/foo/", "^/foo/b\\+r\\.biz$"},
}

func TestExtractRoot(t *testing.T) {
	for _, tt := range extractTests {
		out, err := extractRoot(tt.in)
		if err != nil {
			t.Errorf("extractRoot(%s) returned error %v, want no error", tt.in, err)
		}
		if out != tt.outRoot {
			t.Errorf("extractRoot(%s) => '%s', want '%s'", tt.in, out, tt.outRoot)
		}
	}
}

func TestExtractRegex(t *testing.T) {
	for _, tt := range extractTests {
		out, err := extractRegex(tt.in)
		if err != nil {
			t.Errorf("extractRegex(%s) returned error %v, want no error", tt.in, err)
		}
		if out.String() != tt.outRegex {
			t.Errorf("extractRegex(%s) => '%s', want '%s'", tt.in, out, tt.outRegex)
		}
	}
}
