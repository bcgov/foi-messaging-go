package messaging

import (
	"context"
	"fmt"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

// init registers the hooks this module's testing/ (messagingtest) package
// needs to drive the real publish and consume paths without Redis.
//
// See internal/testseam for why a seam is required at all and why it is
// not an exported API instead. Nothing here widens the library's public
// surface: internal/ is invisible to applications.
func init() {
	testseam.WithCorrelationID = contextWithCorrelationID

	testseam.NewRecordingPublisher = func(source, streamPrefix string,
		record func(ctx context.Context, stream, id string, body []byte,
			metadata map[string]string) error) (any, error) {
		cfg := Config{
			Source:       source,
			StreamPrefix: streamPrefix,
			// Never dialed. NewPublisher performs no connection at
			// construction — redisstream.NewPublisher is struct
			// construction plus config validation, and go-redis connects
			// lazily — and publishFn below replaces the only call that
			// would ever touch the network.
			Redis: RedisConfig{Address: "127.0.0.1:6379"},
		}

		p, err := NewPublisher(cfg)
		if err != nil {
			return nil, fmt.Errorf("building recording publisher: %w", err)
		}
		p.publishFn = record
		return p, nil
	}

	testseam.Dispatch = func(ctx context.Context, consumer any, topic string,
		body []byte, metadata map[string]string, p *testseam.Probe) error {
		c, ok := consumer.(*Consumer)
		if !ok {
			return fmt.Errorf("messaging: testseam.Dispatch wants a *messaging.Consumer, got %T", consumer)
		}
		return c.dispatch(testseam.NewContext(ctx, p), topic, body, metadata)
	}
}
