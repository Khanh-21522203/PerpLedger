package event

import (
	"time"

	"github.com/google/uuid"
)

// WithdrawalRequested represents a user's request to withdraw funds
type WithdrawalRequested struct {
	WithdrawalID uuid.UUID
	UserID       uuid.UUID
	Asset        string
	Amount       int64 // Fixed-point
	Sequence     int64
	Timestamp    time.Time
}

func (w *WithdrawalRequested) IdempotencyKey() string {
	return w.WithdrawalID.String()
}

func (w *WithdrawalRequested) EventType() EventType {
	return EventTypeWithdrawalRequested
}

func (w *WithdrawalRequested) MarketID() *string {
	return nil // Global event
}

func (w *WithdrawalRequested) SourceSequence() int64 {
	return w.Sequence
}

// WithdrawalConfirmed represents custody confirmation of withdrawal
type WithdrawalConfirmed struct {
	WithdrawalID uuid.UUID
	UserID       uuid.UUID
	Asset        string
	Amount       int64
	Sequence     int64
	Timestamp    time.Time
}

func (w *WithdrawalConfirmed) IdempotencyKey() string {
	return w.WithdrawalID.String()
}

func (w *WithdrawalConfirmed) EventType() EventType {
	return EventTypeWithdrawalConfirmed
}

func (w *WithdrawalConfirmed) MarketID() *string {
	return nil
}

func (w *WithdrawalConfirmed) SourceSequence() int64 {
	return w.Sequence
}

// WithdrawalRejected represents custody rejection of withdrawal
type WithdrawalRejected struct {
	WithdrawalID uuid.UUID
	UserID       uuid.UUID
	Asset        string
	Amount       int64
	Reason       string
	Sequence     int64
	Timestamp    time.Time
}

func (w *WithdrawalRejected) IdempotencyKey() string {
	return w.WithdrawalID.String()
}

func (w *WithdrawalRejected) EventType() EventType {
	return EventTypeWithdrawalRejected
}

func (w *WithdrawalRejected) MarketID() *string {
	return nil
}

func (w *WithdrawalRejected) SourceSequence() int64 {
	return w.Sequence
}
