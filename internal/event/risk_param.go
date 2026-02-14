package event

import (
	"fmt"
)

// RiskParamUpdate represents an update to market risk parameters.
// Per doc §3.9 / margin-and-liquidations.md §8: when received, the core
// updates in-memory risk params and recomputes margin for all positions in the market.
type RiskParamUpdate struct {
	Market       string // Market identifier (e.g., "BTC-USDT-PERP")
	IMFraction   int64  // Initial Margin fraction (decimal_precision=6, scale=1_000_000)
	MMFraction   int64  // Maintenance Margin fraction (decimal_precision=6, scale=1_000_000)
	MaxLeverage  int64  // Derived: 1_000_000 / IMFraction
	TickSize     int64  // Minimum price increment
	LotSize      int64  // Minimum quantity increment
	EffectiveSeq int64  // Sequence at which params take effect
	Sequence     int64  // Source sequence
	Timestamp    int64  // Epoch microseconds (versioned input)
}

func (r *RiskParamUpdate) IdempotencyKey() string {
	return fmt.Sprintf("risk_param:%s:%d", r.Market, r.EffectiveSeq)
}

func (r *RiskParamUpdate) EventType() EventType {
	return EventTypeRiskParamUpdate
}

func (r *RiskParamUpdate) MarketID() *string {
	s := r.Market
	return &s
}

func (r *RiskParamUpdate) SourceSequence() int64 {
	return r.Sequence
}
