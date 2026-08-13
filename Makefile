.PHONY: build test test-examples test-all test-integration lint tidy up down

build:
	go build ./...

test:
	go test ./...

# examples/telemetry is its own Go module, so ./... from the root does not
# descend into it. It needs its own invocation, and it is easy to forget:
# it holds the test pinning the exported Prometheus metric names.
test-examples:
	cd examples/telemetry && go test ./...

# Every non-Docker test tier. Use this rather than `test` alone.
test-all: test test-examples

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

up:
	docker compose up -d

down:
	docker compose down
