// internal/query/balance.go (NEW)
package query

import (
	"github.com/google/uuid"
)

// BalanceResponse represents user balance state for API queries
type BalanceResponse struct {
	UserID uuid.UUID `json:"user_id"`
	Asset  string    `json:"asset"`

	// Ledger balances (from journal entries)
	TotalBalance      int64 `json:"total_balance"`      // collateral + reserved
	AvailableBalance  int64 `json:"available_balance"`  // collateral only
	ReservedBalance   int64 `json:"reserved_balance"`   // margin holds
	PendingDeposit    int64 `json:"pending_deposit"`    // unconfirmed deposits
	PendingWithdrawal int64 `json:"pending_withdrawal"` // unconfirmed withdrawals

	// Derived values (computed at query time, NOT ledger balances)
	UnrealizedPnL   int64 `json:"unrealized_pnl"`   // from positions + mark prices
	EffectiveEquity int64 `json:"effective_equity"` // total_balance + unrealized_pnl

	// Metadata
	AsOfSequence int64 `json:"as_of_sequence"` // last applied event sequence
}

// MarginInfo contains derived margin metrics
type MarginInfo struct {
	UserID uuid.UUID `json:"user_id"`

	// Position-level
	TotalNotional int64 `json:"total_notional"` // sum of abs(position.size * mark_price)
	TotalIM       int64 `json:"total_im"`       // required initial margin
	TotalMM       int64 `json:"total_mm"`       // required maintenance margin

	// Account-level
	EffectiveEquity int64 `json:"effective_equity"` // total_balance + unrealized_pnl
	MarginFraction  int64 `json:"margin_fraction"`  // equity / notional (fixed-point)

	// Status
	IsLiquidatable bool `json:"is_liquidatable"` // margin_fraction < MM

	AsOfSequence int64 `json:"as_of_sequence"`
}
