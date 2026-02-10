package ingestion

import (
	"PerpLedger/internal/event"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GRPCIngestService provides admin/manual event injection via gRPC.
// Per doc §15: gRPC ingest is for admin operations and manual event injection,
// not for high-throughput ingestion (use NATS for that).
type GRPCIngestService struct {
	eventChan chan<- event.Event
}

func NewGRPCIngestService(eventChan chan<- event.Event) *GRPCIngestService {
	return &GRPCIngestService{eventChan: eventChan}
}

// InjectDeposit manually injects a DepositConfirmed event.
func (s *GRPCIngestService) InjectDeposit(
	ctx context.Context,
	userID uuid.UUID,
	asset string,
	amount int64,
) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	evt := &event.DepositConfirmed{
		DepositID: uuid.New(),
		UserID:    userID,
		Asset:     asset,
		Amount:    amount,
		Sequence:  time.Now().UnixMicro(), // Admin-injected: use timestamp as sequence
		Timestamp: time.Now(),
	}

	select {
	case s.eventChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InjectWithdrawal manually injects a WithdrawalRequested event.
func (s *GRPCIngestService) InjectWithdrawal(
	ctx context.Context,
	userID uuid.UUID,
	asset string,
	amount int64,
) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	evt := &event.WithdrawalRequested{
		WithdrawalID: uuid.New(),
		UserID:       userID,
		Asset:        asset,
		Amount:       amount,
		Sequence:     time.Now().UnixMicro(),
		Timestamp:    time.Now(),
	}

	select {
	case s.eventChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InjectMarkPrice manually injects a MarkPriceUpdate event.
func (s *GRPCIngestService) InjectMarkPrice(
	ctx context.Context,
	marketID string,
	markPrice int64,
	priceSequence int64,
) error {
	if markPrice <= 0 {
		return fmt.Errorf("mark price must be positive")
	}

	evt := &event.MarkPriceUpdate{
		Market:         marketID,
		MarkPrice:      markPrice,
		PriceSequence:  priceSequence,
		PriceTimestamp: time.Now().UnixMicro(),
		IndexPrice:     markPrice, // Default: same as mark price
	}

	select {
	case s.eventChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InjectFundingSnapshot manually injects a FundingRateSnapshot event.
func (s *GRPCIngestService) InjectFundingSnapshot(
	ctx context.Context,
	marketID string,
	epochID int64,
	fundingRate int64,
	markPrice int64,
) error {
	evt := &event.FundingRateSnapshot{
		Market:      marketID,
		FundingRate: fundingRate,
		EpochID:     epochID,
		MarkPrice:   markPrice,
		EpochTs:     time.Now().UnixMicro(),
	}

	select {
	case s.eventChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

	select {
	case s.eventChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
