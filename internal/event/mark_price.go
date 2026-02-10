// internal/event/mark_price.go (NEW)
package event

import "fmt"

// MarkPriceUpdate represents a mark price update from oracle
type MarkPriceUpdate struct {
	MarketID       string
	MarkPrice      int64 // Fixed-point: price scale
	PriceSequence  int64 // Monotonic per market
	PriceTimestamp int64 // Epoch microseconds (versioned input)
	IndexPrice     int64 // Optional, for reference
}

func (m *MarkPriceUpdate) IdempotencyKey() string {
	return fmt.Sprintf("%s:price:%d", m.MarketID, m.PriceSequence)
}

func (m *MarkPriceUpdate) EventType() EventType {
	return EventTypeMarkPriceUpdate
}

//func (m *MarkPriceUpdate) MarketID() *string {
//	return &m.MarketID
//}

func (m *MarkPriceUpdate) SourceSequence() int64 {
	return m.PriceSequence
}
