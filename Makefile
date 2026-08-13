.PHONY: build test test-examples test-all test-integration test-scripts verify lint tidy up down

# Extra flags for the test tiers. CI and `verify` set -race -count=1; plain
# `make test` stays fast for local iteration.
GOTESTFLAGS ?=

build:
	go build ./...

test:
	go test $(GOTESTFLAGS) ./...

# examples/telemetry is its own Go module, so ./... from the root does not
# descend into it. It needs its own invocation, and it is easy to forget:
# it holds the test pinning the exported Prometheus metric names.
test-examples:
	cd examples/telemetry && go test $(GOTESTFLAGS) ./...

# Every non-Docker test tier. Use this rather than `test` alone.
test-all: test test-examples

test-integration:
	go test -tags=integration $(GOTESTFLAGS) ./...

test-scripts:
	.github/scripts/changelog-section_test.sh

# Exactly what CI runs, in one command. Run this before pushing a release
# tag: a published module version cannot be corrected, only superseded.
#
# The target-specific variable propagates to every prerequisite, so the
# tiers need no duplicate -race targets.
verify: GOTESTFLAGS = -race -count=1
verify: lint test test-examples test-integration test-scripts

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

up:
	docker compose up -d

down:
	docker compose down
