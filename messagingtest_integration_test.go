//go:build integration

package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

type crosscheckPayload struct {
	Value string `json:"value"`
}

var crosscheckDef = messaging.EventDef{
	Topic:   "crosscheck",
	Type:    "crosscheck.event",
	Version: "1.0.0",
}

type crosscheckHandler struct{}

func (crosscheckHandler) Handle(context.Context, messaging.Envelope[crosscheckPayload]) error {
	return messaging.AsPermanent(errors.New("permanently broken"))
}

// The probe must reflect reality, not only itself. This runs one
// permanent-error scenario through real Redis and through Dispatch, and
// asserts the two produce the same DLQ reason.
func TestMessagingTest_MatchesRealRedisDLQReason(t *testing.T) {
	cfg := consumeFixture(t)
	ctx := context.Background()

	// ── the real path ────────────────────────────────────────────────
	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := messaging.RegisterHandler(consumer, crosscheckDef, crosscheckHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	if _, err := publisher.Publish(ctx, crosscheckDef, crosscheckPayload{Value: "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	realReason := awaitDLQReason(t, cfg.Redis.Address, "foi:crosscheck.dlq")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ── the messagingtest path ───────────────────────────────────────
	fake, err := messaging.NewConsumer(messagingtest.Config())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = fake.Close() })
	if err := messaging.RegisterHandler(fake, crosscheckDef, crosscheckHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	e, err := messagingtest.NewEvent(crosscheckDef, crosscheckPayload{Value: "v"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	res, err := messagingtest.Dispatch(ctx, fake, e)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if len(res.DeadLetters) != 1 {
		t.Fatalf("got %d dead letters from Dispatch, want 1", len(res.DeadLetters))
	}
	if res.DeadLetters[0].Reason != realReason {
		t.Fatalf("Dispatch reported %q, real Redis reported %q",
			res.DeadLetters[0].Reason, realReason)
	}
	if res.DeadLetters[0].OriginalTopic != "crosscheck" {
		t.Fatalf("got original topic %q, want crosscheck",
			res.DeadLetters[0].OriginalTopic)
	}
}

// awaitDLQReason polls stream until a dead letter appears and returns its
// reason. The DLQ write and the ack are asynchronous to the publish, so
// polling is what the existing integration tests do here too.
func awaitDLQReason(t *testing.T, addr, stream string) string {
	t.Helper()
	ctx := context.Background()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := testsupport.ReadStreamEntries(ctx, addr, stream)
		if err != nil {
			t.Fatalf("ReadStreamEntries: %v", err)
		}
		if len(entries) > 0 {
			var dl messaging.DeadLetter
			if err := json.Unmarshal([]byte(entries[0].Fields["payload"]), &dl); err != nil {
				t.Fatalf("unmarshalling dead letter: %v", err)
			}
			return dl.Reason
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("no dead letter appeared on %s within 30s", stream)
	return ""
}
