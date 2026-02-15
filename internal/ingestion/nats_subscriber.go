package ingestion

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSSubscriber subscribes to NATS JetStream subjects and feeds events
// into the deterministic core via the eventChan.
// Per doc §15 (Ingest APIs): NATS JetStream is the primary high-throughput
// ingestion surface. Each subject maps to an event type.
type NATSSubscriber struct {
	js        jetstream.JetStream
	eventChan chan<- RawEvent
	consumers []jetstream.ConsumeContext
}

// RawEvent is the parsed-but-untyped event from NATS, ready for the shell
// to validate and convert into a typed event.Event before sending to the core.
type RawEvent struct {
	Subject   string
	Data      []byte
	Timestamp time.Time
	AckFunc   func() // Call to ACK the NATS message after successful processing
	NakFunc   func() // Call to NAK on failure (will be redelivered)
}

// SubjectConfig maps NATS subjects to event types.
// Per doc §15: each event type has its own subject for independent scaling.
type SubjectConfig struct {
	Subject      string
	EventType    string
	ConsumerName string
	StreamName   string
}

// DefaultSubjects returns the standard subject configuration per doc §15.
func DefaultSubjects() []SubjectConfig {
	return []SubjectConfig{
		{Subject: "perp.trades.>", EventType: "TradeFill", ConsumerName: "ledger-trades", StreamName: "PERP_TRADES"},
		{Subject: "perp.deposits.initiated.>", EventType: "DepositInitiated", ConsumerName: "ledger-deposit-init", StreamName: "PERP_DEPOSITS"},
		{Subject: "perp.deposits.confirmed.>", EventType: "DepositConfirmed", ConsumerName: "ledger-deposit-confirm", StreamName: "PERP_DEPOSITS"},
		{Subject: "perp.withdrawals.requested.>", EventType: "WithdrawalRequested", ConsumerName: "ledger-wd-request", StreamName: "PERP_WITHDRAWALS"},
		{Subject: "perp.withdrawals.confirmed.>", EventType: "WithdrawalConfirmed", ConsumerName: "ledger-wd-confirm", StreamName: "PERP_WITHDRAWALS"},
		{Subject: "perp.withdrawals.rejected.>", EventType: "WithdrawalRejected", ConsumerName: "ledger-wd-reject", StreamName: "PERP_WITHDRAWALS"},
		{Subject: "perp.prices.>", EventType: "MarkPriceUpdate", ConsumerName: "ledger-prices", StreamName: "PERP_PRICES"},
		{Subject: "perp.funding.snapshot.>", EventType: "FundingRateSnapshot", ConsumerName: "ledger-funding-snap", StreamName: "PERP_FUNDING"},
		{Subject: "perp.funding.settle.>", EventType: "FundingEpochSettle", ConsumerName: "ledger-funding-settle", StreamName: "PERP_FUNDING"},
		{Subject: "perp.liquidation.fill.>", EventType: "LiquidationFill", ConsumerName: "ledger-liq-fill", StreamName: "PERP_LIQUIDATION"},
		{Subject: "perp.liquidation.completed.>", EventType: "LiquidationCompleted", ConsumerName: "ledger-liq-complete", StreamName: "PERP_LIQUIDATION"},
		{Subject: "perp.risk.params.>", EventType: "RiskParamUpdate", ConsumerName: "ledger-risk-params", StreamName: "PERP_RISK"},
	}
}

func NewNATSSubscriber(js jetstream.JetStream, eventChan chan<- RawEvent) *NATSSubscriber {
	return &NATSSubscriber{
		js:        js,
		eventChan: eventChan,
	}
}

// Subscribe creates JetStream consumers for all configured subjects.
// Consumers use explicit ACK, max_deliver=5, ack_wait=30s.
func (ns *NATSSubscriber) Subscribe(ctx context.Context, subjects []SubjectConfig) error {
	for _, cfg := range subjects {
		consumer, err := ns.js.CreateOrUpdateConsumer(ctx, cfg.StreamName, jetstream.ConsumerConfig{
			Durable:       cfg.ConsumerName,
			FilterSubject: cfg.Subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err != nil {
			return fmt.Errorf("create consumer %s: %w", cfg.ConsumerName, err)
		}

		consumerContext, err := consumer.Consume(func(msg jetstream.Msg) {
			raw := RawEvent{
				Subject:   msg.Subject(),
				Data:      msg.Data(),
				Timestamp: time.Now(),
				AckFunc:   func() { msg.Ack() },
				NakFunc:   func() { msg.Nak() },
			}

			select {
			case ns.eventChan <- raw:
				// Successfully queued for processing
			case <-ctx.Done():
				msg.Nak()
			}
		})
		if err != nil {
			return fmt.Errorf("consume %s: %w", cfg.ConsumerName, err)
		}

		ns.consumers = append(ns.consumers, consumerContext)
		log.Printf("INFO: subscribed to %s (consumer=%s)", cfg.Subject, cfg.ConsumerName)
	}

	return nil
}

// EnsureStreams creates the required JetStream streams if they don't exist.
// Per doc §15: streams use FileStorage, retention=Limits, max_age=72h.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	streams := []jetstream.StreamConfig{
		{
			Name:      "PERP_TRADES",
			Subjects:  []string{"perp.trades.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_DEPOSITS",
			Subjects:  []string{"perp.deposits.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_WITHDRAWALS",
			Subjects:  []string{"perp.withdrawals.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_PRICES",
			Subjects:  []string{"perp.prices.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_FUNDING",
			Subjects:  []string{"perp.funding.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_LIQUIDATION",
			Subjects:  []string{"perp.liquidation.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
		{
			Name:      "PERP_RISK",
			Subjects:  []string{"perp.risk.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    72 * time.Hour,
			Replicas:  1,
		},
	}

	for _, cfg := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
		log.Printf("INFO: ensured stream %s", cfg.Name)
	}

	return nil
}

// Stop gracefully stops all consumers.
func (ns *NATSSubscriber) Stop() {
	for _, cc := range ns.consumers {
		cc.Stop()
	}
	log.Println("INFO: NATS subscribers stopped")
}

// ConnectNATS establishes a NATS connection and returns a JetStream context.
func ConnectNATS(url string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("WARN: NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Println("INFO: NATS reconnected")
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}

	return nc, js, nil
}
