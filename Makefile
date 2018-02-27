BINARY := xrt

BUILD_VERSION := 0.1.0
BUILD_SHA     := $(shell git rev-parse --short HEAD)

LDFLAG_VERSION := main.buildVersion=${BUILD_VERSION}
LDFLAG_SHA     := main.buildSha=${BUILD_SHA}
LDFLAGS        := -ldflags "-X ${LDFLAG_VERSION} -X ${LDFLAG_SHA}"

GOARCH ?= $(shell go env GOARCH)
GOOS   ?= $(shell go env GOOS)

PACKAGE := ${BINARY}-${BUILD_VERSION}-${GOARCH}-${GOOS}

default:
	go build ${LDFLAGS} -o ${BINARY}

test:
	go test

dist: clean test
	mkdir ${PACKAGE}
	cp README.md ${PACKAGE}/README.md
	cp LICENSE ${PACKAGE}/LICENSE
	go build ${LDFLAGS} -o ../${PACKAGE}/${BINARY}
	tar czf ${PACKAGE}.tar.gz ${PACKAGE}
	rm -rf ${PACKAGE}

clean:
	rm -rf ${BINARY}
	rm -f *.tar.gz
