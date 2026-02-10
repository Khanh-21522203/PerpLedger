// internal/event/liquidation.go (NEW)
package event

import (
	"fmt"
	"github.com/google/uuid"
)

// LiquidationTriggered emitted when margin < MM
type LiquidationTriggered struct {
	LiquidationID uuid.UUID
	UserID        uuid.UUID
	MarketID      string
	Sequence      int64
	Timestamp     int64
}

func (l *LiquidationTriggered) IdempotencyKey() string {
	return l.LiquidationID.String()
}

func (l *LiquidationTriggered) EventType() EventType {
	return EventTypeLiquidationTriggered
}

func (l *LiquidationTriggered) MarketID() *string {
	return &l.MarketID
}

func (l *LiquidationTriggered) SourceSequence() int64 {
	return l.Sequence
}

// LiquidationFill represents a fill from liquidation engine
type LiquidationFill struct {
	LiquidationID uuid.UUID
	FillID        uuid.UUID
	UserID        uuid.UUID
	MarketID      string
	Side          Side
	Quantity      int64
	Price         int64
	Fee           int64
	Sequence      int64
	Timestamp     int64
}

func (l *LiquidationFill) IdempotencyKey() string {
	return fmt.Sprintf("%s:%s", l.LiquidationID, l.FillID)
}

func (l *LiquidationFill) EventType() EventType {
	return EventTypeLiquidationFill
}

func (l *LiquidationFill) MarketID() *string {
	return &l.MarketID
}

func (l *LiquidationFill) SourceSequence() int64 {
	return l.Sequence
}

// LiquidationCompleted marks liquidation as finished
type LiquidationCompleted struct {
	LiquidationID uuid.UUID
	UserID        uuid.UUID
	MarketID      string
	Deficit       int64 // If positive, bankruptcy occurred
	Sequence      int64
	Timestamp     int64
}

func (l *LiquidationCompleted) IdempotencyKey() string {
	return fmt.Sprintf("%s:complete", l.LiquidationID)
}

func (l *LiquidationCompleted) EventType() EventType {
	return EventTypeLiquidationCompleted
}

func (l *LiquidationCompleted) MarketID() *string {
	return &l.MarketID
}

func (l *LiquidationCompleted) SourceSequence() int64 {
	return l.Sequence
}
