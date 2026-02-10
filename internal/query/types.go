package query

import "github.com/google/uuid"

// PositionResponse represents a position for API queries.
type PositionResponse struct {
	UserID           uuid.UUID `json:"user_id"`
	MarketID         string    `json:"market_id"`
	Side             int32     `json:"side"`
	Size             int64     `json:"size"`
	AvgEntryPrice    int64     `json:"avg_entry_price"`
	RealizedPnL      int64     `json:"realized_pnl"`
	UnrealizedPnL    int64     `json:"unrealized_pnl"`    // Derived at query time
	LastFundingEpoch int64     `json:"last_funding_epoch"`
	LiquidationState int32     `json:"liquidation_state"`
	Version          int64     `json:"version"`
	AsOfSequence     int64     `json:"as_of_sequence"`
}

// FundingHistoryResponse represents a funding payment record for API queries.
type FundingHistoryResponse struct {
	UserID       uuid.UUID `json:"user_id"`
	MarketID     string    `json:"market_id"`
	EpochID      int64     `json:"epoch_id"`
	FundingRate  int64     `json:"funding_rate"`
	PositionSize int64     `json:"position_size"`
	MarkPrice    int64     `json:"mark_price"`
	Payment      int64     `json:"payment"`
	Timestamp    int64     `json:"timestamp"`
	AsOfSequence int64     `json:"as_of_sequence"`
}

// LiquidationResponse represents liquidation status for API queries.
type LiquidationResponse struct {
	UserID        uuid.UUID `json:"user_id"`
	LiquidationID string    `json:"liquidation_id"`
	MarketID      string    `json:"market_id"`
	TriggeredAt   int64     `json:"triggered_at"`
	InitialSize   int64     `json:"initial_size"`
	RemainingSize int64     `json:"remaining_size"`
	State         int32     `json:"state"`
	Deficit       int64     `json:"deficit"`
}

// JournalHistoryEntry represents a journal entry for API queries.
type JournalHistoryEntry struct {
	JournalID     string `json:"journal_id"`
	BatchID       string `json:"batch_id"`
	EventRef      string `json:"event_ref"`
	Sequence      int64  `json:"sequence"`
	DebitAccount  string `json:"debit_account"`
	CreditAccount string `json:"credit_account"`
	AssetID       uint16 `json:"asset_id"`
	Amount        int64  `json:"amount"`
	JournalType   int32  `json:"journal_type"`
	Timestamp     int64  `json:"timestamp"`
}

// IntegrityReport is the result of an integrity verification check.
type IntegrityReport struct {
	IsHealthy        bool              `json:"is_healthy"`
	HashChainBreaks  []int64           `json:"hash_chain_breaks,omitempty"`
	UnbalancedAssets []UnbalancedAsset `json:"unbalanced_assets,omitempty"`
}

// UnbalancedAsset represents an asset with non-zero global balance sum.
type UnbalancedAsset struct {
	AssetID   uint16 `json:"asset_id"`
	Imbalance int64  `json:"imbalance"`
}
