package event

import (
	"fmt"
)

// FundingRateSnapshot represents funding rate at epoch boundary.
// Idempotency key: "{market}:{epoch_id}" (per doc §3.6).
type FundingRateSnapshot struct {
	Market      string // Market identifier
	FundingRate int64  // Fixed-point: rate scale (decimal_precision=8, scale=100_000_000), signed
	EpochID     int64  // Monotonic per market
	MarkPrice   int64  // Fixed-point: mark price at epoch boundary
	EpochTs     int64  // Epoch boundary timestamp in microseconds (versioned input)
}

func (f *FundingRateSnapshot) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", f.Market, f.EpochID)
}

func (f *FundingRateSnapshot) EventType() EventType {
	return EventTypeFundingRateSnapshot
}

func (f *FundingRateSnapshot) MarketID() *string {
	s := f.Market
	return &s
}

func (f *FundingRateSnapshot) SourceSequence() int64 {
	return f.EpochID
}

// FundingEpochSettle triggers settlement execution for all positions in a market.
// This is an internal event — the core iterates all positions and generates
// one journal batch per user (see doc §8 Funding).
type FundingEpochSettle struct {
	Market  string
	EpochID int64
}

func (f *FundingEpochSettle) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d:settle", f.Market, f.EpochID)
}

func (f *FundingEpochSettle) EventType() EventType {
	return EventTypeFundingEpochSettle
}

func (f *FundingEpochSettle) MarketID() *string {
	s := f.Market
	return &s
}

func (f *FundingEpochSettle) SourceSequence() int64 {
	return f.EpochID
}
