package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
)

const splitSize int64 = 64 << 20 // 64mb

// split ...
type split struct {
	filename string
	start    int64
	end      int64
	err      error
}

// enumerate ...
func enumerate(input string) (chan *split, error) {
	regex := extractRegex(input)
	r, err := regexp.Compile(regex)
	if err != nil {
		return nil, err
	}

	root := extractRoot(input)
	if root == "" {
		if root, err = os.Getwd(); err != nil {
			return nil, err
		}
	}

	c := make(chan *split)

	go func() {
		if err := walk(c, root, r); err != nil {
			c <- &split{
				err: err,
			}
		}
		close(c)
	}()

	return c, nil
}

func extractRoot(input string) string {
	root := ""
	part := ""

	for _, c := range input {
		switch c {
		case '*', '?', '{', '[':
			return root
		case '/':
			root = fmt.Sprintf("%s%s%c", root, part, c)
			part = ""
		default:
			part = fmt.Sprintf("%s%c", part, c)
		}
	}

	return root
}

func extractRegex(input string) string {
	regex := ""

	for _, c := range input {
		switch c {
		case '.', '$', '(', ')', '|', '+':
			regex = fmt.Sprintf("%s%c", regex, '\\')
		case '*':
			regex = fmt.Sprintf("%s%s", regex, "[^/]")
		case '?':
			regex = fmt.Sprintf("%s%c", regex, '.')
			continue
		case '{':
			regex = fmt.Sprintf("%s%s", regex, "(?:")
			continue
		case ',':
			regex = fmt.Sprintf("%s%c", regex, '|')
			continue
		case '}':
			regex = fmt.Sprintf("%s%c", regex, ')')
			continue
		}
		regex = fmt.Sprintf("%s%c", regex, c)
	}

	return regex
}

// walk ...
func walk(c chan *split, filename string, regex *regexp.Regexp) error {
	s, err := os.Stat(filename)
	if err != nil {
		return err
	}

	if s.Mode().IsRegular() && regex.Match([]byte(filename)) {
		s, err := os.Stat(filename)
		if err != nil {
			return err
		}

		var start int64
		for start+splitSize < s.Size() {
			c <- &split{
				filename: filename,
				start:    start,
				end:      start + splitSize,
			}
			start += splitSize
		}

		c <- &split{
			filename: filename,
			start:    start,
			end:      s.Size(),
		}

		return nil
	}

	if s.Mode().IsDir() {
		fis, err := ioutil.ReadDir(filename)
		if err != nil {
			return err
		}

		for _, fi := range fis {
			if err := walk(c, path.Join(filename, fi.Name()), regex); err != nil {
				return err
			}
		}
	}

	return nil
}
