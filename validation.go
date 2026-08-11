package messaging

import (
	"fmt"
	"regexp"
)

var (
	eventTypePattern     = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,2}$`)
	schemaVersionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
)

// validateEnvelope checks required fields, event_type format, schema_version
// format, and timestamp presence. Used at publish time (Task 6) and, in a
// later phase, at consume time.
func validateEnvelope[T any](e Envelope[T]) error {
	if e.EventID == "" {
		return fmt.Errorf("envelope: event_id is required")
	}
	if e.EventType == "" {
		return fmt.Errorf("envelope: event_type is required")
	}
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("envelope: event_type %q must be 2-3 dot-separated lowercase segments", e.EventType)
	}
	if e.SchemaVersion == "" {
		return fmt.Errorf("envelope: schema_version is required")
	}
	if !schemaVersionPattern.MatchString(e.SchemaVersion) {
		return fmt.Errorf("envelope: schema_version %q must be MAJOR.MINOR.PATCH", e.SchemaVersion)
	}
	if e.Source == "" {
		return fmt.Errorf("envelope: source is required")
	}
	if e.CorrelationID == "" {
		return fmt.Errorf("envelope: correlation_id is required")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("envelope: timestamp is required")
	}
	return nil
}
