BINDIR := "build"

build: setup
	CGO_ENABLED=0 gox -osarch="linux/amd64 darwin/amd64" -output="$(BINDIR)/{{.Dir}}_{{.OS}}_{{.Arch}}" ./

setup:
	go get github.com/mitchellh/gox

.DEFAULT: build

.PHONY: build setup
