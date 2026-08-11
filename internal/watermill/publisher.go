package watermill

import (
	"fmt"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	goredis "github.com/redis/go-redis/v9"
)

// Publisher wraps a watermill-redisstream Publisher. Its methods take and
// return only primitive types and byte slices, so no watermill or go-redis
// type crosses into callers outside internal/.
type Publisher struct {
	pub *redisstream.Publisher
}

// NewPublisher builds a Publisher backed by client.
func NewPublisher(client *goredis.Client) (*Publisher, error) {
	pub, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Client:     client,
			Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
		},
		wm.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("creating redis stream publisher: %w", err)
	}

	return &Publisher{pub: pub}, nil
}

// Publish writes payload to topic as a single Redis Streams entry with
// message id id, carrying metadata as Watermill message metadata.
func (p *Publisher) Publish(topic string, id string, payload []byte, metadata map[string]string) error {
	msg := message.NewMessage(id, payload)
	for k, v := range metadata {
		msg.Metadata.Set(k, v)
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("publishing to topic %q: %w", topic, err)
	}
	return nil
}

// Close releases the underlying publisher's resources.
func (p *Publisher) Close() error {
	return p.pub.Close()
}
