package event

import (
	"time"

	"github.com/google/uuid"
)

// DepositInitiated represents a deposit that has been submitted but not yet confirmed.
// Moves funds: external:deposits → user:pending_deposit.
type DepositInitiated struct {
	DepositID uuid.UUID
	UserID    uuid.UUID
	Asset     string
	Amount    int64     // Fixed-point: asset scale, must be > 0
	Sequence  int64     // Source sequence
	Timestamp time.Time // Versioned input timestamp
}

func (d *DepositInitiated) IdempotencyKey() string {
	return d.DepositID.String()
}

func (d *DepositInitiated) EventType() EventType {
	return EventTypeDepositInitiated
}

func (d *DepositInitiated) MarketID() *string {
	return nil // Global event
}

func (d *DepositInitiated) SourceSequence() int64 {
	return d.Sequence
}

// DepositConfirmed represents a deposit that has been confirmed on-chain.
// Moves funds: external:deposits → user:collateral (or pending_deposit → collateral).
type DepositConfirmed struct {
	DepositID uuid.UUID
	UserID    uuid.UUID
	Asset     string
	Amount    int64     // Fixed-point: asset scale, must be > 0
	Sequence  int64     // Source sequence
	Timestamp time.Time // Versioned input timestamp
}

func (d *DepositConfirmed) IdempotencyKey() string {
	return d.DepositID.String()
}

func (d *DepositConfirmed) EventType() EventType {
	return EventTypeDepositConfirmed
}

func (d *DepositConfirmed) MarketID() *string {
	return nil
}

func (d *DepositConfirmed) SourceSequence() int64 {
	return d.Sequence
}
