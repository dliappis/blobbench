build: build-darwin build-linux

build-darwin:
	GOOS=${PLATFORM%/*}
	GOARCH=${PLATFORM#*/}
	mkdir -p "build/darwin-amd64/"
	GOOS=darwin GOARCH=amd64 go build -o build/darwin-amd64/blobbench main.go

build-linux:
	mkdir -p "build/linux-amd64/"
	GOOS=linux GOARCH=amd64 go build -o build/linux-amd64/blobbench main.go

run: build
	build/darwin-amd64/blobbench

.DEFAULT: run
