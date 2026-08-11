// Command consumer shows how a service consumes events with the messaging
// library. It is a compile-checked illustration, not a runnable service.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	messaging "github.com/bcgov/foi-messaging-go"
)

// DocumentCreatedPayload would normally live in the shared contracts
// repository alongside its EventDef.
type DocumentCreatedPayload struct {
	EntityID string `json:"entity_id"`
	Name     string `json:"name"`
}

// DocumentCreated is the event contract: topic, type, and schema version
// declared once and shared by publishers and consumers.
var DocumentCreated = messaging.EventDef{
	Topic:   "documents",
	Type:    "document.created",
	Version: "1.0.0",
}

type documentHandler struct {
	log *slog.Logger
}

// Handle must be idempotent: delivery is at-least-once, so the same event
// may arrive more than once. EventID is the deduplication key.
func (h documentHandler) Handle(_ context.Context, env messaging.Envelope[DocumentCreatedPayload]) error {
	h.log.Info("document created",
		"event_id", env.EventID,
		"correlation_id", env.CorrelationID,
		"name", env.Payload.Name)
	return nil
}

func main() {
	log := slog.Default()

	cfg := messaging.Config{
		Source: "documents.service",
		Redis: messaging.RedisConfig{
			Address:  os.Getenv("REDIS_ADDRESS"),
			Username: os.Getenv("REDIS_USER"),
			Password: os.Getenv("REDIS_PASSWORD"),
		},
		Consumer: messaging.ConsumerConfig{
			Group: "documents-service",
		},
	}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		log.Error("creating consumer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error("closing consumer", "error", err)
		}
	}()

	if err := messaging.RegisterHandler(consumer, DocumentCreated, documentHandler{log: log}); err != nil {
		log.Error("registering handler", "error", err)
		os.Exit(1)
	}

	// Run blocks until the context is cancelled, then drains in-flight
	// handlers before returning.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx); err != nil {
		log.Error("running consumer", "error", err)
		os.Exit(1)
	}
}
