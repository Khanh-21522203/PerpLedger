// internal/state/position.go (UPDATE)
package state

import (
	"PerpLedger/internal/event"
	"github.com/google/uuid"
)

// LiquidationState tracks liquidation progress
type LiquidationState int32

const (
	LiquidationStateHealthy LiquidationState = iota
	LiquidationStateAtRisk
	LiquidationStateInLiquidation
	LiquidationStatePartiallyLiquidated
	LiquidationStateClosed
	LiquidationStateBankrupt
)

// Position represents a user's position in a market
type Position struct {
	UserID           uuid.UUID
	MarketID         string
	Side             event.Side
	Size             int64 // Fixed-point: quantity scale
	AvgEntryPrice    int64 // Fixed-point: price scale
	RealizedPnL      int64 // Fixed-point: quote scale (cumulative)
	LastFundingEpoch int64
	LiquidationState LiquidationState
	LiquidationID    *string // Nullable
	Version          int64   // Optimistic concurrency control
}

func (ls LiquidationState) String() string {
	switch ls {
	case LiquidationStateHealthy:
		return "Healthy"
	case LiquidationStateAtRisk:
		return "AtRisk"
	case LiquidationStateInLiquidation:
		return "InLiquidation"
	case LiquidationStatePartiallyLiquidated:
		return "PartiallyLiquidated"
	case LiquidationStateClosed:
		return "Closed"
	case LiquidationStateBankrupt:
		return "Bankrupt"
	default:
		return "Unknown"
	}
}

// CanTransitionTo validates state transitions
func (ls LiquidationState) CanTransitionTo(next LiquidationState) bool {
	validTransitions := map[LiquidationState][]LiquidationState{
		LiquidationStateHealthy: {
			LiquidationStateAtRisk,
		},
		LiquidationStateAtRisk: {
			LiquidationStateHealthy,
			LiquidationStateInLiquidation,
		},
		LiquidationStateInLiquidation: {
			LiquidationStatePartiallyLiquidated,
			LiquidationStateClosed,
		},
		LiquidationStatePartiallyLiquidated: {
			LiquidationStatePartiallyLiquidated, // Multiple partial fills
			LiquidationStateClosed,
			LiquidationStateHealthy, // Margin recovered
		},
		LiquidationStateClosed: {
			LiquidationStateBankrupt,
		},
		LiquidationStateBankrupt: {
			LiquidationStateHealthy, // After insurance fund coverage
		},
	}

	allowed, ok := validTransitions[ls]
	if !ok {
		return false
	}

	for _, allowedState := range allowed {
		if next == allowedState {
			return true
		}
	}

	return false
}

// IsFlat returns true if position has no exposure
func (p *Position) IsFlat() bool {
	return p.Side == event.SideFlat || p.Size == 0
}

// SideSign returns +1 for long, -1 for short, 0 for flat
func (p *Position) SideSign() int64 {
	switch p.Side {
	case event.SideLong:
		return 1
	case event.SideShort:
		return -1
	default:
		return 0
	}
}

// CanonicalBytes returns deterministic serialization for hashing
func (p *Position) CanonicalBytes() []byte {
	buf := make([]byte, 0, 128)

	// user_id (16 bytes UUID binary)
	buf = append(buf, p.UserID[:]...)

	// market_id (length-prefixed)
	buf = append(buf, byte(len(p.MarketID)))
	buf = append(buf, []byte(p.MarketID)...)

	// side (1 byte)
	buf = append(buf, byte(p.Side))

	// size (8 bytes LE)
	buf = appendInt64LE(buf, p.Size)

	// avg_entry_price (8 bytes LE)
	buf = appendInt64LE(buf, p.AvgEntryPrice)

	// realized_pnl (8 bytes LE)
	buf = appendInt64LE(buf, p.RealizedPnL)

	// last_funding_epoch (8 bytes LE)
	buf = appendInt64LE(buf, p.LastFundingEpoch)

	// liquidation_state (1 byte)
	buf = append(buf, byte(p.LiquidationState))

	return buf
}

func appendInt64LE(buf []byte, v int64) []byte {
	return append(buf,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}
