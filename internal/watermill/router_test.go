package watermill

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRouter_DeliversPayloadAndMetadataToHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", `{"k":"v"}`))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	router, err := NewRouter(time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	type received struct {
		payload  string
		attempt  string
		streamID string
	}
	got := make(chan received, 1)

	router.AddHandler("h", "stream", sub, func(_ context.Context, payload []byte, metadata map[string]string) error {
		got <- received{
			payload:  string(payload),
			attempt:  metadata[MetadataDeliveryAttempt],
			streamID: metadata[MetadataStreamID],
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := router.Run(ctx); err != nil {
			t.Errorf("router.Run: %v", err)
		}
	}()

	select {
	case r := <-got:
		if r.payload != `{"k":"v"}` {
			t.Errorf("payload = %q, want %q", r.payload, `{"k":"v"}`)
		}
		if r.attempt != "1" {
			t.Errorf("attempt = %q, want %q", r.attempt, "1")
		}
		if r.streamID != "1-0" {
			t.Errorf("streamID = %q, want %q", r.streamID, "1-0")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to run")
	}

	cancel()
	if err := router.Close(); err != nil {
		t.Errorf("router.Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("sub.Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for router.Run to complete")
	}
}

func TestRouter_HandlerErrorLeavesEntryUnacked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	router, err := NewRouter(time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	called := make(chan struct{}, 1)
	router.AddHandler("h", "stream", sub, func(context.Context, []byte, map[string]string) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return errors.New("handler failed")
	})

	go func() { _ = router.Run(ctx) }()

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to run")
	}

	time.Sleep(200 * time.Millisecond)
	if ids := reader.ackedIDs(); len(ids) != 0 {
		t.Errorf("acked = %v, want none — a failed handler must leave the entry pending", ids)
	}

	cancel()
	_ = router.Close()
	_ = sub.Close()
}
