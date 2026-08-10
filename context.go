package messaging

// Correlation ID propagation through context.Context will live here:
// reading an inbound correlation ID and placing it for outbound
// publishes within the same handler chain.
// See PRD §5 (Correlation ID Semantics). Not yet implemented — Phase 0
// scaffolding only.
