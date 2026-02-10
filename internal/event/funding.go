// internal/event/funding.go (NEW)
package event

import (
	"fmt"
)

// FundingRateSnapshot represents funding rate at epoch boundary
type FundingRateSnapshot struct {
	MarketID    string
	FundingRate int64 // Fixed-point: rate scale (decimal_precision=8, scale=100_000_000)
	EpochID     int64
	MarkPrice   int64 // Fixed-point: price scale
	EpochTs     int64 // Epoch boundary timestamp (versioned input)
}

func (f *FundingRateSnapshot) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", f.MarketID, f.EpochID)
}

func (f *FundingRateSnapshot) EventType() EventType {
	return EventTypeFundingRateSnapshot
}

//func (f *FundingRateSnapshot) MarketID() *string {
//	return &f.MarketID
//}

func (f *FundingRateSnapshot) SourceSequence() int64 {
	return f.EpochID
}

// FundingEpochSettle triggers settlement execution
type FundingEpochSettle struct {
	MarketID string
	EpochID  int64
}

func (f *FundingEpochSettle) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d:settle", f.MarketID, f.EpochID)
}

func (f *FundingEpochSettle) EventType() EventType {
	return EventTypeFundingEpochSettle
}

//func (f *FundingEpochSettle) MarketID() *string {
//	return &f.MarketID
//}

func (f *FundingEpochSettle) SourceSequence() int64 {
	return f.EpochID
}
