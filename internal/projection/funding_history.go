// internal/projection/funding_history.go (NEW)
package projection

import (
	"github.com/google/uuid"
)

// FundingHistoryEntry represents a user's funding payment record
type FundingHistoryEntry struct {
	UserID       uuid.UUID
	MarketID     string
	EpochID      int64
	FundingRate  int64
	PositionSize int64
	MarkPrice    int64
	Payment      int64 // Signed: positive = paid, negative = received
	JournalID    string
	Timestamp    int64
}

// FundingHistoryProjection maintains queryable funding history
type FundingHistoryProjection struct {
	entries []FundingHistoryEntry
}

func NewFundingHistoryProjection() *FundingHistoryProjection {
	return &FundingHistoryProjection{
		entries: make([]FundingHistoryEntry, 0),
	}
}

// AddEntry records a funding payment
func (p *FundingHistoryProjection) AddEntry(entry FundingHistoryEntry) {
	p.entries = append(p.entries, entry)
}

// QueryByUser returns funding history for a user
func (p *FundingHistoryProjection) QueryByUser(userID uuid.UUID, limit int) []FundingHistoryEntry {
	result := make([]FundingHistoryEntry, 0)

	for i := len(p.entries) - 1; i >= 0 && len(result) < limit; i-- {
		if p.entries[i].UserID == userID {
			result = append(result, p.entries[i])
		}
	}

	return result
}
