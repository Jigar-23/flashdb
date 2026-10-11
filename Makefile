BINARY_NAME=flashdb-server
CLI_NAME=flashdb-cli
GO_BIN=/Users/jigar/.local/go/bin/go

.PHONY: all build run test test-race bench clean docker-build

all: build test

build:
	$(GO_BIN) build -o bin/$(BINARY_NAME) ./cmd/flashdb-server
	$(GO_BIN) build -o bin/$(CLI_NAME) ./cmd/flashdb-cli

run: build
	./bin/$(BINARY_NAME) --port 6379

test:
	$(GO_BIN) test -v ./...

test-race:
	$(GO_BIN) test -race -v ./...

bench:
	$(GO_BIN) test -bench=. -benchmem ./...

clean:
	rm -rf bin/ *.aof *.rdb

docker-build:
	docker build -t flashdb:latest .
