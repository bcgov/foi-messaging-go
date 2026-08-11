// Package testsupport provides infrastructure helpers for this
// repository's own integration tests. It is internal plumbing, not the
// PRD's application-facing testing/ (messagingtest) package, and must
// never be imported by application code.
package testsupport

import (
	"context"
	"fmt"

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
		return "", nil, fmt.Errorf("resolving redis container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return "", nil, fmt.Errorf("resolving redis container port: %w", err)
	}

	terminate = func(ctx context.Context) error {
		return container.Terminate(ctx)
	}

	return fmt.Sprintf("%s:%s", host, port.Port()), terminate, nil
}
