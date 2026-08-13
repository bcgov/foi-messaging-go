package testseam_test

import (
	"context"
	"testing"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

func TestFromContext_RoundTrip(t *testing.T) {
	p := &testseam.Probe{Kind: testseam.KindProcessed}

	got := testseam.FromContext(testseam.NewContext(context.Background(), p))

	if got != p {
		t.Fatalf("got %p, want %p", got, p)
	}
}

// The production case: nothing outside this module's tests places a probe,
// so every delivery in a real service takes this path.
func TestFromContext_AbsentIsNil(t *testing.T) {
	if got := testseam.FromContext(context.Background()); got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}
