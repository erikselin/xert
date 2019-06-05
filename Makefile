BINARY := xrt

BUILD_VERSION := 0.4.0
BUILD_DIR := build

LDFLAG_VERSION := main.version=${BUILD_VERSION}
LDFLAGS        := -ldflags "-X ${LDFLAG_VERSION}"

GOARCH ?= $(shell go env GOARCH)
GOOS   ?= $(shell go env GOOS)

PACKAGE := ${BINARY}-${BUILD_VERSION}-${GOARCH}-${GOOS}

default:
	mkdir ${BUILD_DIR}
	cd cli && go build ${LDFLAGS} -o ../${BUILD_DIR}/${BINARY}

test: default
	go test -v
	cd cli && go test -v
	tests/run 4 1000 16m 16k

dist:
	mkdir ${PACKAGE}
	cp cli/README.md ${PACKAGE}/README.md
	cp LICENSE ${PACKAGE}/LICENSE
	cd cli && go build ${LDFLAGS} -o ../${PACKAGE}/${BINARY}
	tar czf ${PACKAGE}.tar.gz ${PACKAGE}
	rm -rf ${PACKAGE}

clean:
	rm -rf ${BUILD_DIR}
	rm -f *.tar.gz
	rm -rf tests/data-*
	rm -f tests/log-*
	rm -rf tests/output-*
