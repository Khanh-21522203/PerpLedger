package event

import (
	"fmt"

	"github.com/google/uuid"
)

// LiquidationTriggered emitted when margin fraction < MM fraction.
// Published to NATS for the liquidation engine to begin unwinding.
type LiquidationTriggered struct {
	LiquidationID uuid.UUID
	UserID        uuid.UUID
	Market        string // Market identifier
	Sequence      int64
	Timestamp     int64 // Epoch microseconds
}

func (l *LiquidationTriggered) IdempotencyKey() string {
	return l.LiquidationID.String()
}

func (l *LiquidationTriggered) EventType() EventType {
	return EventTypeLiquidationTriggered
}

func (l *LiquidationTriggered) MarketID() *string {
	s := l.Market
	return &s
}

func (l *LiquidationTriggered) SourceSequence() int64 {
	return l.Sequence
}

// LiquidationFill represents a fill from the liquidation engine.
// Idempotency key: "{liquidation_id}:{fill_id}" (per doc §3.8).
type LiquidationFill struct {
	LiquidationID uuid.UUID
	FillID        uuid.UUID
	UserID        uuid.UUID
	Market        string // Market identifier
	Side          Side   // Closing side
	Quantity      int64  // Fixed-point: quantity scale
	Price         int64  // Fixed-point: price scale
	Fee           int64  // Fixed-point: quote scale
	Sequence      int64
	Timestamp     int64 // Epoch microseconds
}

func (l *LiquidationFill) IdempotencyKey() string {
	return fmt.Sprintf("%s:%s", l.LiquidationID, l.FillID)
}

func (l *LiquidationFill) EventType() EventType {
	return EventTypeLiquidationFill
}

func (l *LiquidationFill) MarketID() *string {
	s := l.Market
	return &s
}

func (l *LiquidationFill) SourceSequence() int64 {
	return l.Sequence
}

// LiquidationCompleted marks liquidation as finished.
// If Deficit > 0, bankruptcy occurred and insurance fund was tapped.
type LiquidationCompleted struct {
	LiquidationID uuid.UUID
	UserID        uuid.UUID
	Market        string // Market identifier
	Deficit       int64  // If positive, bankruptcy occurred
	Sequence      int64
	Timestamp     int64 // Epoch microseconds
}

func (l *LiquidationCompleted) IdempotencyKey() string {
	return fmt.Sprintf("%s:complete", l.LiquidationID)
}

func (l *LiquidationCompleted) EventType() EventType {
	return EventTypeLiquidationCompleted
}

func (l *LiquidationCompleted) MarketID() *string {
	s := l.Market
	return &s
}

func (l *LiquidationCompleted) SourceSequence() int64 {
	return l.Sequence
}
