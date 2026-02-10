// internal/event/deposit.go
package event

import "github.com/google/uuid"

type DepositInitiated struct {
	DepositID uuid.UUID
	UserID    uuid.UUID
	Asset     string
	Amount    int64 // Fixed-point
	Sequence  int64
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

type DepositConfirmed struct {
	DepositID uuid.UUID
	UserID    uuid.UUID
	Asset     string
	Amount    int64
	Sequence  int64
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
