package event

import "fmt"

// MarkPriceUpdate represents a mark price update from oracle.
// Idempotency key: "{market}:price:{price_sequence}" (per doc §3.5).
type MarkPriceUpdate struct {
	Market         string // Market identifier (e.g. "BTC-USDT-PERP")
	MarkPrice      int64  // Fixed-point: price scale
	PriceSequence  int64  // Monotonic per market
	PriceTimestamp int64  // Epoch microseconds (versioned input — NOT wall-clock)
	IndexPrice     int64  // Optional, for reference
}

func (m *MarkPriceUpdate) IdempotencyKey() string {
	return fmt.Sprintf("%s:price:%d", m.Market, m.PriceSequence)
}

func (m *MarkPriceUpdate) EventType() EventType {
	return EventTypeMarkPriceUpdate
}

func (m *MarkPriceUpdate) MarketID() *string {
	s := m.Market
	return &s
}

func (m *MarkPriceUpdate) SourceSequence() int64 {
	return m.PriceSequence
}
