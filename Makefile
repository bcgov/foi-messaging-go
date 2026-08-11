.PHONY: build test test-integration lint tidy up down

build:
	go build ./...

test:
	go test ./...

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
