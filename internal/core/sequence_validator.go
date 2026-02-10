// internal/core/sequence_validator.go (NEW)
package core

import (
	"fmt"
	"sync"
)

// SequenceValidator validates source sequences per partition
type SequenceValidator struct {
	expectedNextSeq map[string]int64 // partition -> next expected sequence
	mu              sync.RWMutex
	metrics         *SequenceMetrics
}

func NewSequenceValidator() *SequenceValidator {
	return &SequenceValidator{
		expectedNextSeq: make(map[string]int64),
		metrics:         NewSequenceMetrics(),
	}
}

// ValidateSequence checks source sequence ordering
func (sv *SequenceValidator) ValidateSequence(
	partition string,
	sourceSequence int64,
	idempotencyKey string,
	isDuplicate bool,
) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	expected := sv.expectedNextSeq[partition]

	if sourceSequence < expected {
		// Stale or duplicate
		if isDuplicate {
			// This is expected - already processed
			return nil
		}
		// Out-of-order delivery of NEW event
		sv.metrics.RecordOutOfOrder(partition)
		return fmt.Errorf("out-of-order event: partition=%s, expected=%d, got=%d",
			partition, expected, sourceSequence)
	}

	if sourceSequence == expected {
		// Normal case - advance sequence
		sv.expectedNextSeq[partition] = expected + 1
		return nil
	}

	// sourceSequence > expected - gap detected
	sv.metrics.RecordGap(partition, expected, sourceSequence)
	return fmt.Errorf("sequence gap: partition=%s, expected=%d, got=%d",
		partition, expected, sourceSequence)
}

// ValidatePriceSequence validates price updates (gaps tolerated)
func (sv *SequenceValidator) ValidatePriceSequence(
	marketID string,
	priceSequence int64,
) error {
	partition := fmt.Sprintf("price:%s", marketID)

	sv.mu.Lock()
	defer sv.mu.Unlock()

	expected := sv.expectedNextSeq[partition]

	if priceSequence <= expected {
		// Stale - silently ignore (idempotent)
		return nil
	}

	if priceSequence > expected+1 {
		// Gap detected - log warning but accept
		sv.metrics.RecordPriceGap(marketID, expected, priceSequence)
		// Continue processing - price gaps are tolerable
	}

	// Update expected
	sv.expectedNextSeq[partition] = priceSequence + 1

	return nil
}

// GetExpectedSequence returns next expected sequence for a partition
func (sv *SequenceValidator) GetExpectedSequence(partition string) int64 {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	return sv.expectedNextSeq[partition]
}

// SetExpectedSequence initializes expected sequence (used during recovery)
func (sv *SequenceValidator) SetExpectedSequence(partition string, seq int64) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.expectedNextSeq[partition] = seq
}

// --- Metrics ---

type SequenceMetrics struct {
	gaps       map[string]int64 // partition -> gap count
	outOfOrder map[string]int64 // partition -> out-of-order count
	priceGaps  map[string]int64 // market_id -> price gap count
	mu         sync.RWMutex
}

func NewSequenceMetrics() *SequenceMetrics {
	return &SequenceMetrics{
		gaps:       make(map[string]int64),
		outOfOrder: make(map[string]int64),
		priceGaps:  make(map[string]int64),
	}
}

func (m *SequenceMetrics) RecordGap(partition string, expected, got int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gaps[partition]++
}

func (m *SequenceMetrics) RecordOutOfOrder(partition string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outOfOrder[partition]++
}

func (m *SequenceMetrics) RecordPriceGap(marketID string, expected, got int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.priceGaps[marketID]++
}

func (m *SequenceMetrics) GetGaps(partition string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gaps[partition]
}

func (m *SequenceMetrics) GetOutOfOrder(partition string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.outOfOrder[partition]
}

func (m *SequenceMetrics) GetPriceGaps(marketID string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.priceGaps[marketID]
}
