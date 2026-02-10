package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// OutboundPublisher publishes processed events to NATS for downstream consumers.
// Per doc §15: outbound events are published after persistence is confirmed.
// Subjects follow the pattern: perp.ledger.events.{event_type}
type OutboundPublisher struct {
	js        jetstream.JetStream
	inputChan <-chan PublishableEvent
}

// PublishableEvent is a processed event ready for outbound publishing.
type PublishableEvent struct {
	Sequence       int64       `json:"sequence"`
	EventType      string      `json:"event_type"`
	IdempotencyKey string      `json:"idempotency_key"`
	MarketID       *string     `json:"market_id,omitempty"`
	Payload        interface{} `json:"payload"`
	StateHash      []byte      `json:"state_hash"`
	Timestamp      time.Time   `json:"timestamp"`
}

func NewOutboundPublisher(js jetstream.JetStream, inputChan <-chan PublishableEvent) *OutboundPublisher {
	return &OutboundPublisher{
		js:        js,
		inputChan: inputChan,
	}
}

// Run starts the outbound publisher loop.
// Per doc §15: publishes to perp.ledger.events.{event_type}.{market_id}
func (op *OutboundPublisher) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case evt, ok := <-op.inputChan:
			if !ok {
				return nil
			}

			if err := op.publish(ctx, evt); err != nil {
				log.Printf("WARN: outbound publish failed seq=%d: %v", evt.Sequence, err)
				// Non-fatal: downstream consumers can query the event log directly
			}
		}
	}
}

func (op *OutboundPublisher) publish(ctx context.Context, evt PublishableEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Build subject: perp.ledger.events.{event_type}.{market_id}
	subject := fmt.Sprintf("perp.ledger.events.%s", evt.EventType)
	if evt.MarketID != nil {
		subject = fmt.Sprintf("%s.%s", subject, *evt.MarketID)
	}

	_, err = op.js.Publish(ctx, subject, data)
	return err
}

// EnsureOutboundStream creates the outbound events stream.
func EnsureOutboundStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "PERP_LEDGER_EVENTS",
		Subjects:  []string{"perp.ledger.events.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    72 * time.Hour,
		Replicas:  1,
	})
	if err != nil {
		return fmt.Errorf("create outbound stream: %w", err)
	}
	log.Println("INFO: ensured outbound stream PERP_LEDGER_EVENTS")
	return nil
}
