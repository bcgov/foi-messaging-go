//go:build integration

package watermill_test

import (
	"context"
	"testing"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// redisAddr starts one Redis container shared by every case in the suite.
func redisAddr(t *testing.T) string {
	t.Helper()
	// Not t.Context(): it is cancelled before cleanups run, so terminating
	// the container with it would always fail.
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})
	return addr
}

// TestPubSubConformance runs watermill's universal Pub/Sub suite — the same
// suite watermill-redisstream runs against its own subscriber — over our
// subscriber, paired with the upstream redisstream publisher so wire
// compatibility is exercised too.
func TestPubSubConformance(t *testing.T) {
	addr := redisAddr(t)

	newPubSub := func(t *testing.T, group string) (message.Publisher, message.Subscriber) {
		t.Helper()

		pubClient := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
		pub, err := redisstream.NewPublisher(
			redisstream.PublisherConfig{
				Client:     pubClient,
				Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
			},
			wm.NewStdLogger(false, false),
		)
		if err != nil {
			t.Fatalf("NewPublisher: %v", err)
		}

		subClient := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
		reader := internalredis.NewStreamReader(subClient, group, wm.NewShortUUID())
		sub, err := internalwatermill.NewSubscriber(internalwatermill.SubscriberOptions{
			Reader:        reader,
			Concurrency:   1,
			ClaimInterval: 3 * time.Second,
			ClaimMinIdle:  5 * time.Second,
			BlockTime:     10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}

		return pub, sub
	}

	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    true,
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return newPubSub(t, wm.NewShortUUID())
		},
		newPubSub,
	)
}
