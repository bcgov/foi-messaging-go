// Package testsupport provides infrastructure helpers for this
// repository's own integration tests. It is internal plumbing, not the
// PRD's application-facing testing/ (messagingtest) package, and must
// never be imported by application code.
package testsupport

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartRedis launches a disposable Redis 7 container for integration
// tests and returns its connection address (host:port) and a terminate
// function the caller must invoke to tear the container down.
func StartRedis(ctx context.Context) (addr string, terminate func(context.Context) error, err error) {
	container, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		return "", nil, fmt.Errorf("starting redis container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		hostErr := fmt.Errorf("resolving redis container host: %w", err)
		if termErr := container.Terminate(ctx); termErr != nil {
			return "", nil, errors.Join(hostErr, fmt.Errorf("terminating redis container after host resolution failure: %w", termErr))
		}
		return "", nil, hostErr
	}

	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		portErr := fmt.Errorf("resolving redis container port: %w", err)
		if termErr := container.Terminate(ctx); termErr != nil {
			return "", nil, errors.Join(portErr, fmt.Errorf("terminating redis container after port resolution failure: %w", termErr))
		}
		return "", nil, portErr
	}

	terminate = func(ctx context.Context) error {
		return container.Terminate(ctx)
	}

	return fmt.Sprintf("%s:%s", host, port.Port()), terminate, nil
}

// StreamEntry is a single raw Redis Streams entry.
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// ReadStreamEntries connects to addr and reads every entry currently on
// stream, for integration-test assertions against what a Publisher wrote.
func ReadStreamEntries(ctx context.Context, addr string, stream string) ([]StreamEntry, error) {
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	raw, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("reading stream %q: %w", stream, err)
	}

	entries := make([]StreamEntry, 0, len(raw))
	for _, msg := range raw {
		fields := make(map[string]string, len(msg.Values))
		for k, v := range msg.Values {
			if s, ok := v.(string); ok {
				fields[k] = s
			}
		}
		entries = append(entries, StreamEntry{ID: msg.ID, Fields: fields})
	}
	return entries, nil
}
