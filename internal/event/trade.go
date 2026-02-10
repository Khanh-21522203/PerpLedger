package event

import (
	"time"

	"github.com/google/uuid"
)

// Side represents trade direction
type Side int32

const (
	SideFlat Side = iota
	SideLong
	SideShort
)

// TradeFill represents a matched trade from the matching engine.
// Idempotency key: fill_id (UUID from matching engine).
type TradeFill struct {
	FillID       uuid.UUID // Idempotency key
	UserID       uuid.UUID
	Market       string
	TradeSide    Side
	Quantity     int64     // Fixed-point: quantity scale (decimal_precision=6, scale=1_000_000)
	Price        int64     // Fixed-point: price scale (decimal_precision=2, scale=100)
	Fee          int64     // Fixed-point: quote scale (decimal_precision=6, scale=1_000_000)
	OrderID      uuid.UUID // Reference to originating order
	FillSequence int64     // Source sequence from matching engine
	Timestamp    time.Time // Versioned input timestamp (NOT wall-clock)
}

func (t *TradeFill) IdempotencyKey() string {
	return t.FillID.String()
}

func (t *TradeFill) EventType() EventType {
	return EventTypeTradeFill
}

func (t *TradeFill) MarketID() *string {
	m := t.Market
	return &m
}

func (t *TradeFill) SourceSequence() int64 {
	return t.FillSequence
}
