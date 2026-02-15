package ingestion

import (
	"PerpLedger/internal/event"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrResourceExhausted is returned when the event channel is full.
// Per flow grpc-ingest-flowchart: non-blocking send returns RESOURCE_EXHAUSTED if full.
var ErrResourceExhausted = fmt.Errorf("RESOURCE_EXHAUSTED: event channel is full, try again later")

// GRPCIngestService provides admin/manual event injection via gRPC.
// Per doc §15: gRPC ingest is for admin operations and manual event injection,
// not for high-throughput ingestion (use NATS for that).
type GRPCIngestService struct {
	eventChan chan<- event.Event
}

func NewGRPCIngestService(eventChan chan<- event.Event) *GRPCIngestService {
	return &GRPCIngestService{eventChan: eventChan}
}

// EventChan returns the event channel for external injection (e.g., gRPC SubmitEvent).
func (s *GRPCIngestService) EventChan() chan<- event.Event {
	return s.eventChan
}

// trySend attempts a non-blocking send to the event channel.
// Per flow grpc-ingest-flowchart: returns RESOURCE_EXHAUSTED if channel is full.
func (s *GRPCIngestService) trySend(ctx context.Context, evt event.Event) error {
	select {
	case s.eventChan <- evt:
		return nil
	default:
		return ErrResourceExhausted
	}
}

// InjectDeposit manually injects a DepositConfirmed event.
// Per docs §3.3: all timestamps must be versioned inputs, not wall-clock.
func (s *GRPCIngestService) InjectDeposit(
	ctx context.Context,
	userID uuid.UUID,
	asset string,
	amount int64,
	timestamp time.Time,
) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	evt := &event.DepositConfirmed{
		DepositID: uuid.New(),
		UserID:    userID,
		Asset:     asset,
		Amount:    amount,
		Sequence:  timestamp.UnixMicro(),
		Timestamp: timestamp,
	}

	return s.trySend(ctx, evt)
}

// InjectWithdrawal manually injects a WithdrawalRequested event.
func (s *GRPCIngestService) InjectWithdrawal(
	ctx context.Context,
	userID uuid.UUID,
	asset string,
	amount int64,
	timestamp time.Time,
) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	evt := &event.WithdrawalRequested{
		WithdrawalID: uuid.New(),
		UserID:       userID,
		Asset:        asset,
		Amount:       amount,
		Sequence:     timestamp.UnixMicro(),
		Timestamp:    timestamp,
	}

	return s.trySend(ctx, evt)
}

// InjectMarkPrice manually injects a MarkPriceUpdate event.
func (s *GRPCIngestService) InjectMarkPrice(
	ctx context.Context,
	marketID string,
	markPrice int64,
	priceSequence int64,
	timestampUs int64,
) error {
	if markPrice <= 0 {
		return fmt.Errorf("mark price must be positive")
	}

	evt := &event.MarkPriceUpdate{
		Market:         marketID,
		MarkPrice:      markPrice,
		PriceSequence:  priceSequence,
		PriceTimestamp: timestampUs,
		IndexPrice:     markPrice, // Default: same as mark price
	}

	return s.trySend(ctx, evt)
}

// InjectFundingSnapshot manually injects a FundingRateSnapshot event.
func (s *GRPCIngestService) InjectFundingSnapshot(
	ctx context.Context,
	marketID string,
	epochID int64,
	fundingRate int64,
	markPrice int64,
	epochTimestampUs int64,
) error {
	evt := &event.FundingRateSnapshot{
		Market:      marketID,
		FundingRate: fundingRate,
		EpochID:     epochID,
		MarkPrice:   markPrice,
		EpochTs:     epochTimestampUs,
	}

	return s.trySend(ctx, evt)
}

// InjectFundingSettle manually injects a FundingEpochSettle event.
func (s *GRPCIngestService) InjectFundingSettle(
	ctx context.Context,
	marketID string,
	epochID int64,
) error {
	evt := &event.FundingEpochSettle{
		Market:  marketID,
		EpochID: epochID,
	}

	return s.trySend(ctx, evt)
}
