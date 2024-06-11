SOURCE_FILES?=$$(go list ./...)
TEST_PATTERN?=.
TEST_OPTIONS?=

test:
	@go test $(TEST_OPTIONS) -cover $(SOURCE_FILES) -run $(TEST_PATTERN) -timeout=30s

bench:
	@go test $(TEST_OPTIONS) -cover $(SOURCE_FILES) -bench $(TEST_PATTERN) -timeout=30s

lint:
	@golangci-lint run

build:
	go build -o db-diff -ldflags "main.Version=$(shell git describe --tags --always)"

.DEFAULT_GOAL := build
