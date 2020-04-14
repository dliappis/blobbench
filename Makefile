BINDIR := "build"

build:
	gox -osarch="linux/amd64 darwin/amd64" -output="$(BINDIR)/{{.Dir}}_{{.OS}}_{{.Arch}}" ./

.DEFAULT: build
.PHONY: build
