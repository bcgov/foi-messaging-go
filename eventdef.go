package messaging

// EventDef identifies an event contract: which topic it publishes on,
// its event type, and its schema version. Declared once per event in the
// contract package that owns the payload type, and shared by publishers
// and consumers.
type EventDef struct {
	Topic   string
	Type    string
	Version string
}
