package messaging

import (
	"errors"
	"fmt"
	"testing"
)

var errSentinel = errors.New("sentinel")

func TestClassification_PredicatesMatchConstructors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
		retryable bool
		discard   bool
	}{
		{"permanent", AsPermanent(errSentinel), true, false, false},
		{"retryable", AsRetryable(errSentinel), false, true, false},
		{"discard", AsDiscard(errSentinel), false, false, true},
		// PRD §15: an unwrapped error is retryable by default. Misclassifying
		// a transient failure as permanent silently loses work to the DLQ;
		// the reverse is caught by the delivery-attempt cap.
		{"unclassified", errSentinel, false, true, false},
		{"nil", nil, false, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanent(tc.err); got != tc.permanent {
				t.Errorf("IsPermanent = %v, want %v", got, tc.permanent)
			}
			if got := IsRetryable(tc.err); got != tc.retryable {
				t.Errorf("IsRetryable = %v, want %v", got, tc.retryable)
			}
			if got := IsDiscard(tc.err); got != tc.discard {
				t.Errorf("IsDiscard = %v, want %v", got, tc.discard)
			}
		})
	}
}

func TestClassification_PreservesTheWrappedError(t *testing.T) {
	err := AsPermanent(fmt.Errorf("loading account: %w", errSentinel))

	if !errors.Is(err, errSentinel) {
		t.Error("errors.Is must see through the classification wrapper")
	}
	if got, want := err.Error(), "loading account: sentinel"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestClassification_SurvivesOuterWrapping(t *testing.T) {
	// Handlers classify, then a caller adds context with %w. The
	// classification must still be visible or every wrapped permanent
	// failure would silently become retryable and burn the whole cap.
	err := fmt.Errorf("processing document 42: %w", AsPermanent(errSentinel))

	if !IsPermanent(err) {
		t.Error("IsPermanent must find a classification nested under %w wrapping")
	}
	if IsRetryable(err) {
		t.Error("a nested permanent error must not report as retryable")
	}
}

func TestClassification_OutermostWins(t *testing.T) {
	// Re-classifying is legal: an outer layer that knows more overrides an
	// inner verdict. Documented so the behaviour is not accidental.
	err := AsDiscard(AsPermanent(errSentinel))

	if !IsDiscard(err) {
		t.Error("the outermost classification must win")
	}
	if IsPermanent(err) {
		t.Error("the overridden inner classification must not leak through")
	}
}

func TestClassification_NilStaysNil(t *testing.T) {
	// Wrapping "no error" as a failure would turn a successful handler
	// return into a dead letter.
	if AsPermanent(nil) != nil {
		t.Error("AsPermanent(nil) must be nil")
	}
	if AsRetryable(nil) != nil {
		t.Error("AsRetryable(nil) must be nil")
	}
	if AsDiscard(nil) != nil {
		t.Error("AsDiscard(nil) must be nil")
	}
}
