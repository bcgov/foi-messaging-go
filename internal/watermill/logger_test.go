package watermill

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	wm "github.com/ThreeDotsLabs/watermill"
)

// capturingHandler records the records written to it so tests can assert on
// what the library actually logged.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Sufficient for these tests: attributes added via With are folded into
	// each record as it arrives.
	return &attrHandler{parent: h, attrs: attrs}
}

func (h *capturingHandler) WithGroup(string) slog.Handler { return h }

func (h *capturingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

// find returns the first captured record with the given message.
func (h *capturingHandler) find(msg string) (slog.Record, bool) {
	for _, r := range h.snapshot() {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

type attrHandler struct {
	parent *capturingHandler
	attrs  []slog.Attr
}

func (h *attrHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *attrHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(h.attrs...)
	return h.parent.Handle(ctx, r)
}

func (h *attrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &attrHandler{parent: h.parent, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}

func (h *attrHandler) WithGroup(string) slog.Handler { return h }

// attrValue reads a single attribute off a record.
func attrValue(r slog.Record, key string) (slog.Value, bool) {
	var (
		v     slog.Value
		found bool
	)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

func TestSlogAdapter_MapsErrorFieldsAndLevel(t *testing.T) {
	capture := &capturingHandler{}
	adapter := newLoggerAdapter(slog.New(capture))

	sentinel := errors.New("boom")
	adapter.Error("Handler returned error", sentinel, wm.LogFields{
		"handler_name": "documents",
		"topic":        "foi:documents",
	})

	r, ok := capture.find(logPrefix + "Handler returned error")
	if !ok {
		t.Fatalf("no record for the adapted error; got %+v", capture.snapshot())
	}
	if r.Level != slog.LevelError {
		t.Errorf("level = %v, want %v", r.Level, slog.LevelError)
	}
	if v, ok := attrValue(r, "handler_name"); !ok || v.String() != "documents" {
		t.Errorf("handler_name = %v (present=%v), want documents", v, ok)
	}
	if v, ok := attrValue(r, "error"); !ok || v.Any() != error(sentinel) {
		t.Errorf("error attr = %v (present=%v), want %v", v, ok, sentinel)
	}
}

func TestSlogAdapter_TraceMapsToDebugAndWithCarriesFields(t *testing.T) {
	capture := &capturingHandler{}
	adapter := newLoggerAdapter(slog.New(capture)).With(wm.LogFields{"topic": "foi:documents"})

	adapter.Trace("Received message", nil)

	r, ok := capture.find(logPrefix + "Received message")
	if !ok {
		t.Fatalf("no record for the adapted trace; got %+v", capture.snapshot())
	}
	if r.Level != slog.LevelDebug {
		t.Errorf("level = %v, want %v — slog has no level below debug", r.Level, slog.LevelDebug)
	}
	if v, ok := attrValue(r, "topic"); !ok || v.String() != "foi:documents" {
		t.Errorf("topic = %v (present=%v), want foi:documents — With must carry its fields", v, ok)
	}
}

func TestNewLoggerAdapter_NilLoggerFallsBackToDefault(t *testing.T) {
	if newLoggerAdapter(nil) == nil {
		t.Fatal("newLoggerAdapter(nil) must return a usable adapter, not nil")
	}
}
