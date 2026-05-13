default: help

PROJECT_NAME = $(shell basename "$(PWD)")
BIN_PATH = ./bin
CMD_PATH = ./cmd
TST_PATH = ./test

# =====
# BUILD

## build: build binary on current platform
.PHONY: build
build:
	go build -o bin/cruise-lug

## install: install cli for local use
.PHONY: install
install:
	go install

## build: build binaries for different os and platforms
.PHONY: compile
compile:
	# linux
	GOOS=linux GOARCH=arm go build -o ${BIN_PATH}/${PROJECT_NAME}-arm
	GOOS=linux GOARCH=arm64 go build -o ${BIN_PATH}/${PROJECT_NAME}-arm64
	# macos
	GOARCH=arm GOOS=darwin go build -o ${BIN_PATH}/${PROJECT_NAME}-arm-darwin
	GOARCH=amd64 GOOS=darwin go build -o ${BIN_PATH}/${PROJECT_NAME}-amd64-darwin
	# windows
	GOARCH=amd64 GOOS=windows go build -o ${BIN_PATH}/${PROJECT_NAME}-windows.exe

## clean: clean the workspace and code
.PHONY: clean
clean:
	rm -rf ${BIN_PATH}
	rm -rf ${TST_PATH}/smoke/*
	go vet
	go fmt
	go mod tidy

# ====
# TEST

.PHONY: test
test:
	echo "TODO test"

.PHONY: it
it:
	test
	echo "TODO integration test"

.PHONY: smoke
smoke:
	$(MAKE) clean
	go run main.go -kv -p=8 mb survey AT43-02 ./test/smoke

# ====
# DIST

## dep-check: check dependencies for vulnerabilities
.PHONY: dep-check
dep-check:
	echo "TODO dep-check"

# build project and publish
.PHONY: publish
publish: it
	echo "TODO publish"


# =====
# USAGE

# get help
.PHONY: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECT_NAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo