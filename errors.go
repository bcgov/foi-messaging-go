package messaging

import "errors"

// classification is the failure category a handler error carries.
// Applications attach one by wrapping with AsPermanent, AsRetryable, or
// AsDiscard; the consume path reads it back with the Is* predicates.
// See PRD §15.
type classification int

const (
	classPermanent classification = iota + 1
	classRetryable
	classDiscard
)

// classifiedError carries a classification alongside the error it wraps.
//
// Unwrap is what lets errors.Is and errors.As chain straight through it. A
// handler that wraps its own sentinel must stay matchable by its caller, so
// classification has to be additive rather than a replacement.
type classifiedError struct {
	class classification
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// AsPermanent marks err as a failure that will not resolve on retry — a
// validation failure, an unresolvable reference. The consume path routes it
// straight to the DLQ and acks, skipping both immediate retry and the
// delivery-attempt cap.
//
// A nil err returns nil: wrapping "no error" would turn a successful
// handler return into a dead letter.
func AsPermanent(err error) error { return classify(classPermanent, err) }

// AsRetryable marks err as transient. It is optional — an unclassified
// error is already retryable (PRD §15) — and exists so handlers can say so
// deliberately rather than by omission.
func AsRetryable(err error) error { return classify(classRetryable, err) }

// AsDiscard marks err as a failure the application wants dropped: the event
// is logged at warn and acked, with no retry and no DLQ entry.
func AsDiscard(err error) error { return classify(classDiscard, err) }

func classify(c classification, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{class: c, err: err}
}

// classOf returns the outermost classification in err's chain.
//
// Outermost wins: errors.As walks from the outside in, so an outer layer
// that re-classifies overrides an inner verdict. That is the useful
// direction — the outer layer has strictly more context — and fixing it in
// one place keeps all three predicates consistent.
func classOf(err error) (classification, bool) {
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class, true
	}
	return 0, false
}

// IsPermanent reports whether err is classified permanent.
func IsPermanent(err error) bool {
	c, ok := classOf(err)
	return ok && c == classPermanent
}

// IsDiscard reports whether err is classified discard.
func IsDiscard(err error) bool {
	c, ok := classOf(err)
	return ok && c == classDiscard
}

// IsRetryable reports whether err should be retried, which an unclassified
// error is by default (PRD §15).
//
// The retry loop does not call this — retrying is its fallthrough — but
// operational tooling and tests do, and the default has to be stated
// somewhere executable rather than only in a comment.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	c, ok := classOf(err)
	return !ok || c == classRetryable
}
