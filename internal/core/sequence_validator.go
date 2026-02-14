package core

import (
	"fmt"
)

// SequenceValidator validates source sequences per partition.
// Not thread-safe — only accessed from the single-threaded deterministic core.
type SequenceValidator struct {
	expectedNextSeq map[string]int64 // partition -> next expected sequence
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
	return sv.expectedNextSeq[partition]
}

// SetExpectedSequence initializes expected sequence (used during recovery)
func (sv *SequenceValidator) SetExpectedSequence(partition string, seq int64) {
	sv.expectedNextSeq[partition] = seq
}

// --- Metrics ---

// SequenceMetrics tracks sequence validation stats.
// Not thread-safe — only accessed from the single-threaded deterministic core.
type SequenceMetrics struct {
	gaps       map[string]int64 // partition -> gap count
	outOfOrder map[string]int64 // partition -> out-of-order count
	priceGaps  map[string]int64 // market_id -> price gap count
}

func NewSequenceMetrics() *SequenceMetrics {
	return &SequenceMetrics{
		gaps:       make(map[string]int64),
		outOfOrder: make(map[string]int64),
		priceGaps:  make(map[string]int64),
	}
}

func (m *SequenceMetrics) RecordGap(partition string, expected, got int64) {
	m.gaps[partition]++
}

func (m *SequenceMetrics) RecordOutOfOrder(partition string) {
	m.outOfOrder[partition]++
}

func (m *SequenceMetrics) RecordPriceGap(marketID string, expected, got int64) {
	m.priceGaps[marketID]++
}

func (m *SequenceMetrics) GetGaps(partition string) int64 {
	return m.gaps[partition]
}

func (m *SequenceMetrics) GetOutOfOrder(partition string) int64 {
	return m.outOfOrder[partition]
}

func (m *SequenceMetrics) GetPriceGaps(marketID string) int64 {
	return m.priceGaps[marketID]
}
