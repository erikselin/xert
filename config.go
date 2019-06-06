package xrt

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
)

// config ...
type config struct {
	input        string
	mappers      int
	memoryString string
	output       string
	reducers     int
	tempDir      string
	inputRoot    string
	inputRegex   *regexp.Regexp
	memory       int
	tempRoot     string
	tempScratch  string
	tempSpill    string
	tempOutput   string
}

func newConfig(input, output string, reducer bool, config Config) (*config, error) {
	var err error
	conf := &config{
		input:        input,
		mappers:      config.Mappers,
		memoryString: config.Memory,
		output:       output,
		reducers:     config.Reducers,
		tempDir:      config.TempDir,
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
	if len(conf.memoryString) == 0 {
		conf.memoryString = defaultMemoryString
	}
	if conf.memory = parseMemory(conf.memoryString); conf.memory < 0 {
		return nil, fmt.Errorf(
			"error parsing memory string: \"%s\"",
			conf.memoryString,
		)
	}
	if conf.mappers <= 0 {
		conf.mappers = defaultMappers
	}
	if conf.reducers <= 0 {
		conf.reducers = defaultReducers
	}
	if conf.hasOutput() {
		if conf.output, err = filepath.Abs(conf.output); err != nil {
			return nil, fmt.Errorf("error parsing output: %v", err)
		}
	}
	return conf, nil
}

func (c *config) hasInput() bool {
	return len(c.input) > 0
}

func (c *config) hasOutput() bool {
	return len(c.output) > 0
}

func configFileSystem(conf *config) error {
	if _, err := os.Stat(conf.output); c.hasOutput() && err == nil {
		return nil, fmt.Errorf(
			"%s: --%s directory %s already exists",
			conf.arg0,
			argOutput,
			conf.output,
		)
	}
	var err error
	if conf.tempDir, err = ioutil.TempDir(conf.tempRoot, "xrt-"); err != nil {
		return fmt.Errorf(
			"xrt: failed creating new directory in '%s' - %v",
			conf.tempRoot,
			err,
		)
	}
	conf.tempScratch = path.Join(conf.tempDir, "scratch")
	if err := os.Mkdir(conf.tempScratch, 0700); err != nil {
		return fmt.Errorf(
			"xrt: failed setup of temporary scratch directory '%s' - %v",
			conf.tempScratch,
			err,
		)
	}
	if conf.hasOutput() {
		conf.tempOutput = path.Join(conf.tempDir, "output")
		if err := os.Mkdir(conf.tempOutput, 0700); err != nil {
			return fmt.Errorf(
				"xrt: failed setup of temporary output directory '%s' - %v",
				conf.tempOutput,
				err,
			)
		}
	}
	if conf.hasReducer() {
		conf.tempSpill = path.Join(conf.tempDir, "spill")
		if err = os.Mkdir(conf.tempSpill, 0700); err != nil {
			return fmt.Errorf(
				"xrt: failed setup of temporary spill directory '%s' - %v",
				conf.tempSpill,
				err,
			)
		}
	}
	return nil
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
