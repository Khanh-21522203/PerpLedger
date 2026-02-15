package state

import (
	"fmt"
)

// FundingManager tracks funding epochs and snapshots
type FundingManager struct {
	snapshots         map[string]*FundingSnapshot // key: "market_id:epoch_id"
	expectedNextEpoch map[string]int64            // market_id -> next epoch_id
}

type FundingSnapshot struct {
	MarketID    string
	EpochID     int64
	FundingRate int64
	MarkPrice   int64
	Timestamp   int64
}

func NewFundingManager() *FundingManager {
	return &FundingManager{
		snapshots:         make(map[string]*FundingSnapshot),
		expectedNextEpoch: make(map[string]int64),
	}
}

// StoreFundingSnapshot validates and stores a funding snapshot
func (fm *FundingManager) StoreFundingSnapshot(
	marketID string,
	epochID int64,
	fundingRate int64,
	markPrice int64,
	timestamp int64,
) error {
	expected := fm.expectedNextEpoch[marketID]

	if epochID < expected {
		// Duplicate - skip (idempotent)
		return nil
	}

	if epochID > expected {
		// Gap detected
		return fmt.Errorf("funding epoch gap for %s: expected=%d, got=%d",
			marketID, expected, epochID)
	}

	// Store snapshot
	key := fmt.Sprintf("%s:%d", marketID, epochID)
	fm.snapshots[key] = &FundingSnapshot{
		MarketID:    marketID,
		EpochID:     epochID,
		FundingRate: fundingRate,
		MarkPrice:   markPrice,
		Timestamp:   timestamp,
	}

	fm.expectedNextEpoch[marketID] = epochID + 1

	return nil
}

// GetFundingSnapshot retrieves a stored snapshot
func (fm *FundingManager) GetFundingSnapshot(marketID string, epochID int64) (*FundingSnapshot, bool) {
	key := fmt.Sprintf("%s:%d", marketID, epochID)
	snap, ok := fm.snapshots[key]
	return snap, ok
}

// RestoreSnapshot directly sets a funding snapshot (used for snapshot restore)
func (fm *FundingManager) RestoreSnapshot(snap *FundingSnapshot) {
	key := fmt.Sprintf("%s:%d", snap.MarketID, snap.EpochID)
	fm.snapshots[key] = snap
}

// RestoreNextEpoch directly sets the next expected epoch (used for snapshot restore)
func (fm *FundingManager) RestoreNextEpoch(marketID string, nextEpoch int64) {
	fm.expectedNextEpoch[marketID] = nextEpoch
}

// GetAllSnapshots returns all funding snapshots (for snapshot creation)
func (fm *FundingManager) GetAllSnapshots() map[string]*FundingSnapshot {
	result := make(map[string]*FundingSnapshot, len(fm.snapshots))
	for k, v := range fm.snapshots {
		result[k] = v
	}
	return result
}

// GetAllNextEpochs returns all next epoch IDs (for snapshot creation)
func (fm *FundingManager) GetAllNextEpochs() map[string]int64 {
	result := make(map[string]int64, len(fm.expectedNextEpoch))
	for k, v := range fm.expectedNextEpoch {
		result[k] = v
	}
	return result
}
