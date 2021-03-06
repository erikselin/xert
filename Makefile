BINARY := xrt

BUILD_VERSION := 0.3.4

LDFLAG_VERSION := main.version=${BUILD_VERSION}
LDFLAGS        := -ldflags "-X ${LDFLAG_VERSION}"

GOARCH ?= $(shell go env GOARCH)
GOOS   ?= $(shell go env GOOS)

PACKAGE := ${BINARY}-${BUILD_VERSION}-${GOARCH}-${GOOS}

default:
	go build ${LDFLAGS} -o ${BINARY}

test: default
	go test -v
	tests/run

dist:
	mkdir ${PACKAGE}
	cp README.md ${PACKAGE}/README.md
	cp LICENSE ${PACKAGE}/LICENSE
	go build ${LDFLAGS} -o ${PACKAGE}/${BINARY}
	tar czf ${PACKAGE}.tar.gz ${PACKAGE}
	rm -rf ${PACKAGE}

clean:
	rm -rf ${BINARY}
	rm -f *.tar.gz
	rm -f tests/log-*
	rm -rf tests/output-*
	rm -rf tests/input
