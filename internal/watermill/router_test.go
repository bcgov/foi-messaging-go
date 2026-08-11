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

	router, err := NewRouter(time.Second, nil)
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

// TestRouter_InFlightHandlerKeepsLiveContextDuringDrain is the fast,
// Redis-free form of the drain guarantee, and covers the two places it can
// be broken: deriving the message context from the subscribe context, and
// letting watermill close the shared Subscriber the moment the router
// context is cancelled (message/router.go's handleClose), which closes the
// shutdown signal that settles in-flight messages.
func TestRouter_InFlightHandlerKeepsLiveContextDuringDrain(t *testing.T) {
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

	// Well above the handler's 300ms of work.
	router, err := NewRouter(5*time.Second, nil)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	started := make(chan struct{})
	handlerErr := make(chan error, 1)
	router.AddHandler("h", "stream", sub, func(hctx context.Context, _ []byte, _ map[string]string) error {
		close(started)
		select {
		case <-hctx.Done():
			handlerErr <- hctx.Err()
		case <-time.After(300 * time.Millisecond):
			handlerErr <- nil
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
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to start")
	}

	// Shut down while the handler is mid-flight.
	cancel()

	select {
	case err := <-handlerErr:
		if err != nil {
			t.Errorf("handler context was cancelled with %v; an in-flight handler must keep "+
				"a live context for the whole close timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to finish")
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for router.Run to return")
	}

	if err := router.Close(); err != nil {
		t.Errorf("router.Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("sub.Close: %v", err)
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

	router, err := NewRouter(time.Second, nil)
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
