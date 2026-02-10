// internal/event/trade.go
package event

import (
	"github.com/google/uuid"
)

// Side represents trade direction
type Side int32

const (
	SideFlat Side = iota
	SideLong
	SideShort
)

// TradeFill represents a matched trade from the matching engine
type TradeFill struct {
	FillID       uuid.UUID // Idempotency key
	UserID       uuid.UUID
	Market       string
	TradeSide    Side
	Quantity     int64 // Fixed-point: quantity * 1e8
	Price        int64 // Fixed-point: price * 1e8
	Fee          int64 // Fixed-point: fee * 1e8
	OrderID      uuid.UUID
	FillSequence int64 // Source sequence from matching engine
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
