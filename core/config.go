package core

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
)

const (
	argInput        = "input"
	argMapper       = "mapper"
	argMappers      = "mappers"
	argMemoryString = "memory"
	argOutput       = "output"
	argProfile      = "profile"
	argReducer      = "reducer"
	argReducers     = "reducers"
	argShowVersion  = "version"
	argTempDir      = "tempdir"
)

var (
	// cli flag defaults
	defaultMappers      = 1
	defaultReducers     = 1
	defaultMemoryString = "16m"
	defaultTempDir      = os.TempDir()
)

// config ...
type config struct {
	// name of the program using the xrt core package
	arg0 string

	// set through cli flags
	input        string
	mapper       string
	mappers      int
	memoryString string
	output       string
	reducer      string
	reducers     int
	showVersion  bool
	showHelp     bool
	tempRoot     string

	// constructed from other configurations
	inputRoot   string
	inputRegex  *regexp.Regexp
	memory      int
	tempDir     string
	tempScratch string
	tempSpill   string
	tempOutput  string
}

// hasInput ...
func (conf *config) hasInput() bool {
	return len(conf.input) > 0
}

// hasReducer ...
func (conf *config) hasReducer() bool {
	return len(conf.reducer) > 0
}

// hasOutput ...
func (conf *config) hasOutput() bool {
	return len(conf.output) > 0
}

// configParse ...
func configParse(args []string) (*config, error) {
	conf := &config{arg0: args[0]}
	fs := flag.NewFlagSet(conf.arg0, flag.ContinueOnError)
	var (
		input        = fs.String(argInput, "", "")
		mapper       = fs.String(argMapper, "", "")
		mappers      = fs.Int(argMappers, defaultMappers, "")
		memoryString = fs.String(argMemoryString, defaultMemoryString, "")
		output       = fs.String(argOutput, "", "")
		reducer      = fs.String(argReducer, "", "")
		reducers     = fs.Int(argReducers, defaultReducers, "")
		showVersion  = fs.Bool(argShowVersion, false, "")
		tempRoot     = fs.String(argTempDir, defaultTempDir, "")
	)
	if len(args) <= 1 {
		conf.showHelp = true
		return conf, nil
	}
	err := flag.CommandLine.Parse(args[1:])
	if err == flag.ErrHelp {
		conf.showHelp = true
		return conf, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %v", conf.arg0, err)
	}
	if *showVersion {
		conf.showVersion = true
		return conf, nil
	}
	conf.input = *input
	if conf.hasInput() {
		if conf.input, err = filepath.Abs(conf.input); err != nil {
			return nil, fmt.Errorf(
				"parsing --%s failed with error: %v",
				argInput,
				err,
			)
		}
		if conf.inputRegex, err = extractRegex(conf.input); err != nil {
			return nil, fmt.Errorf(
				"parsing --%s failed with error: %v",
				argInput,
				err,
			)
		}
		if conf.inputRoot, err = extractRoot(conf.input); err != nil {
			return nil, fmt.Errorf(
				"parsing --%s failed with error: %v",
				argInput,
				err,
			)
		}
	}
	if conf.mapper = *mapper; len(mapper) == 0 {
		return nil, fmt.Errorf("%s: --%s is required", conf.arg0, argMapper)
	}
	if conf.mappers = *mappers; conf.mappers <= 0 {
		return nil, fmt.Errorf(
			"%s: invalid argument --%s=%d",
			conf.arg0,
			argMappers,
			conf.mappers,
		)
	}
	conf.memoryString = *memoryString
	conf.output = *output
	conf.reducer = *reducer
	if conf.reducers = *reducers; conf.hasReducer() && conf.reducers <= 0 {
		return nil, fmt.Errorf(
			"%s: invalid argument --%s=%d",
			conf.arg0,
			argReducers,
			conf.reducers,
		)
	}
	conf.tempRoot = *tempRoot
	if conf.memory = parseMemory(*memoryString); conf.memory < 0 {
		return nil, fmt.Errorf(
			"%s: invalid argument --%s=%s",
			conf.arg0,
			argMemoryString,
			conf.memoryString,
		)
	}
	return conf, nil
}

// configHelp prints the available flags and associated defaults.
func configHelp() {
	fmt.Printf("usage: %s [--help] [--%s] <arguments>\n", arg0, argShowVersion)
	fmt.Printf(
		" --%s <input>   Input pattern, example: path/to/file_*.tsv\n",
		argInput)
	fmt.Printf(" --%s <cmd>    Mapper command (required)\n", argMapper)
	fmt.Printf(
		" --%s <num>   Number of mappers (default: %d)\n",
		argMappers,
		defaultMappers,
	)
	fmt.Printf(
		" --%s <mem>    Memory limit, example: 1k, 2m, 3g, 4t (default: %s)\n",
		argMemoryString,
		defaultMemoryString,
	)
	fmt.Printf(
		" --%s <dir>    Output directory, if not set output goes to stdout\n",
		argOutput,
	)
	fmt.Printf(
		" --%s <cmd>   Reducer command, do not set for a map-only job\n",
		argReducer,
	)
	fmt.Printf(
		" --%s <num>  Number of reducers (default: %d)\n",
		argReducers,
		defaultReducers,
	)
	fmt.Printf(
		" --%s <dir>   Temporary directory (default : %s)\n",
		argTempDir,
		defaultTempDir,
	)
}

// configFileSystem prepares the file system for running the xrt core job.
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
