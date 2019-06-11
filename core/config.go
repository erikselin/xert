package xrt

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// config ...
type config struct {
	startTime    time.Time
	rollbackOnce *sync.Once

	input        string
	mappers      int
	memoryString string
	output       string
	reducers     int
	showVersion  bool
	tempDir      string

	inputRoot   string
	inputRegex  *regexp.Regexp
	memory      int
	tempRoot    string
	tempScratch string
	tempSpill   string
	tempOutput  string
}

func (c *config) hasInput() bool {
	return len(c.input) > 0
}

func (c *config) hasOutput() bool {
	return len(c.output) > 0
}

func (c *config) hasReducer() bool {
	return c.reducers > 0
}

func newConfig(
	input string,
	mappers int,
	memoryString string,
	output string,
	reducers int,
	showVersion bool,
	tempDir string,
) (*config, error) {
	var err error
	if showVersion {
		return &config{showVersion: true}, nil
	}
	conf := &config{
		startTime:    time.Now(),
		rollbackOnce: &sync.Once{},
		input:        input,
		mappers:      mappers,
		memoryString: memoryString,
		output:       output,
		reducers:     reducers,
		showVersion:  showVersion,
		tempDir:      tempDir,
	}
	if conf.hasInput() {
		if conf.input, err = filepath.Abs(conf.input); err != nil {
			return nil, fmt.Errorf("error parsing input: %v", err)
		}
		if conf.inputRegex, err = extractRegex(conf.input); err != nil {
			return nil, fmt.Errorf("error parsing input: %v", err)
		}
		if conf.inputRoot, err = extractRoot(conf.input); err != nil {
			return nil, fmt.Errorf("error parsing input: %v", err)
		}
	}
	if conf.mappers <= 0 {
		return nil, fmt.Errorf(
			"mappers=%d must be set to one or more",
			conf.mappers,
		)
	}
	if conf.memory = parseMemory(conf.memoryString); conf.memory < 0 {
		return nil, fmt.Errorf(
			"error parsing memory=\"%s\"",
			conf.memoryString,
		)
	}
	if _, err = os.Stat(conf.tempDir); err != nil {
		return nil, fmt.Errorf("error accessing tempDir: %v", err)
	}
	if conf.tempRoot, err = ioutil.TempDir(conf.tempDir, "xrt-"); err != nil {
		return nil, err
	}
	conf.tempScratch = path.Join(conf.tempRoot, "scratch")
	if err := os.Mkdir(conf.tempScratch, 0700); err != nil {
		return nil, err
	}
	if conf.hasOutput() {
		if conf.output, err = filepath.Abs(conf.output); err != nil {
			return nil, fmt.Errorf("error parsing output: %v", err)
		}
		if _, err := os.Stat(conf.output); err == nil {
			return nil, fmt.Errorf("output=\"%s\" already exists", conf.output)
		}
	}
	conf.tempOutput = path.Join(conf.tempRoot, "output")
	if err := os.Mkdir(conf.tempOutput, 0700); err != nil {
		return nil, err
	}
	conf.tempSpill = path.Join(conf.tempRoot, "spill")
	if err = os.Mkdir(conf.tempSpill, 0700); err != nil {
		return nil, err
	}
	return conf, nil
}

// extractRegex ...
func extractRegex(input string) (*regexp.Regexp, error) {
	regex := "^"
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
	return regexp.Compile(fmt.Sprintf("%s$", regex))
}

// extractRoot ...
func extractRoot(input string) (string, error) {
	root := ""
	part := ""
	for _, c := range input {
		switch c {
		case '*', '?', '{', '[':
			return root, nil
		case '/':
			root = fmt.Sprintf("%s%s%c", root, part, c)
			part = ""
		default:
			part = fmt.Sprintf("%s%c", part, c)
		}
	}
	if root == "" {
		return os.Getwd()
	}
	return root, nil
}

// parseMemory takes a string representing a memory amount and converts it into
// a integer representign the number of bytes.
// For example:
//    parseMemory("1k") = 1024
//    parseMemory("1m") = 1048576
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
