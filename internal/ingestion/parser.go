package ingestion

import (
	"PerpLedger/internal/event"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ParseRawEvent converts a RawEvent (JSON bytes + event type string) into a typed event.Event.
// Per doc §15: the ingestion shell validates, parses, and converts raw events before
// sending to the deterministic core.
func ParseRawEvent(raw RawEvent, eventType string) (event.Event, error) {
	switch eventType {
	case "TradeFill":
		return parseTradeFill(raw.Data)
	case "DepositInitiated":
		return parseDepositInitiated(raw.Data)
	case "DepositConfirmed":
		return parseDepositConfirmed(raw.Data)
	case "WithdrawalRequested":
		return parseWithdrawalRequested(raw.Data)
	case "WithdrawalConfirmed":
		return parseWithdrawalConfirmed(raw.Data)
	case "WithdrawalRejected":
		return parseWithdrawalRejected(raw.Data)
	case "MarkPriceUpdate":
		return parseMarkPriceUpdate(raw.Data)
	case "FundingRateSnapshot":
		return parseFundingRateSnapshot(raw.Data)
	case "FundingEpochSettle":
		return parseFundingEpochSettle(raw.Data)
	case "LiquidationFill":
		return parseLiquidationFill(raw.Data)
	case "LiquidationCompleted":
		return parseLiquidationCompleted(raw.Data)
	case "RiskParamUpdate":
		return parseRiskParamUpdate(raw.Data)
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

// --- JSON wire formats ---
// These structs represent the JSON payloads received from NATS.
// Field names use snake_case to match upstream producers.

type tradeFillJSON struct {
	FillID       string `json:"fill_id"`
	UserID       string `json:"user_id"`
	Market       string `json:"market"`
	Side         string `json:"side"` // "long" or "short"
	Quantity     int64  `json:"quantity"`
	Price        int64  `json:"price"`
	Fee          int64  `json:"fee"`
	OrderID      string `json:"order_id"`
	FillSequence int64  `json:"fill_sequence"`
	TimestampUs  int64  `json:"timestamp_us"`
}

func parseTradeFill(data []byte) (*event.TradeFill, error) {
	var j tradeFillJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse TradeFill: %w", err)
	}

	fillID, err := uuid.Parse(j.FillID)
	if err != nil {
		return nil, fmt.Errorf("parse fill_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	orderID, err := uuid.Parse(j.OrderID)
	if err != nil {
		return nil, fmt.Errorf("parse order_id: %w", err)
	}

	side := event.SideLong
	if j.Side == "short" {
		side = event.SideShort
	}

	return &event.TradeFill{
		FillID:       fillID,
		UserID:       userID,
		Market:       j.Market,
		TradeSide:    side,
		Quantity:     j.Quantity,
		Price:        j.Price,
		Fee:          j.Fee,
		OrderID:      orderID,
		FillSequence: j.FillSequence,
		Timestamp:    time.UnixMicro(j.TimestampUs),
	}, nil
}

type depositJSON struct {
	DepositID   string `json:"deposit_id"`
	UserID      string `json:"user_id"`
	Asset       string `json:"asset"`
	Amount      int64  `json:"amount"`
	Sequence    int64  `json:"sequence"`
	TimestampUs int64  `json:"timestamp_us"`
}

func parseDepositInitiated(data []byte) (*event.DepositInitiated, error) {
	var j depositJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse DepositInitiated: %w", err)
	}
	depositID, err := uuid.Parse(j.DepositID)
	if err != nil {
		return nil, fmt.Errorf("parse deposit_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.DepositInitiated{
		DepositID: depositID,
		UserID:    userID,
		Asset:     j.Asset,
		Amount:    j.Amount,
		Sequence:  j.Sequence,
		Timestamp: time.UnixMicro(j.TimestampUs),
	}, nil
}

func parseDepositConfirmed(data []byte) (*event.DepositConfirmed, error) {
	var j depositJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse DepositConfirmed: %w", err)
	}
	depositID, err := uuid.Parse(j.DepositID)
	if err != nil {
		return nil, fmt.Errorf("parse deposit_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.DepositConfirmed{
		DepositID: depositID,
		UserID:    userID,
		Asset:     j.Asset,
		Amount:    j.Amount,
		Sequence:  j.Sequence,
		Timestamp: time.UnixMicro(j.TimestampUs),
	}, nil
}

type withdrawalJSON struct {
	WithdrawalID string `json:"withdrawal_id"`
	UserID       string `json:"user_id"`
	Asset        string `json:"asset"`
	Amount       int64  `json:"amount"`
	Sequence     int64  `json:"sequence"`
	TimestampUs  int64  `json:"timestamp_us"`
	Reason       string `json:"reason,omitempty"`
}

func parseWithdrawalRequested(data []byte) (*event.WithdrawalRequested, error) {
	var j withdrawalJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse WithdrawalRequested: %w", err)
	}
	wdID, err := uuid.Parse(j.WithdrawalID)
	if err != nil {
		return nil, fmt.Errorf("parse withdrawal_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.WithdrawalRequested{
		WithdrawalID: wdID,
		UserID:       userID,
		Asset:        j.Asset,
		Amount:       j.Amount,
		Sequence:     j.Sequence,
		Timestamp:    time.UnixMicro(j.TimestampUs),
	}, nil
}

func parseWithdrawalConfirmed(data []byte) (*event.WithdrawalConfirmed, error) {
	var j withdrawalJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse WithdrawalConfirmed: %w", err)
	}
	wdID, err := uuid.Parse(j.WithdrawalID)
	if err != nil {
		return nil, fmt.Errorf("parse withdrawal_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.WithdrawalConfirmed{
		WithdrawalID: wdID,
		UserID:       userID,
		Asset:        j.Asset,
		Amount:       j.Amount,
		Sequence:     j.Sequence,
		Timestamp:    time.UnixMicro(j.TimestampUs),
	}, nil
}

func parseWithdrawalRejected(data []byte) (*event.WithdrawalRejected, error) {
	var j withdrawalJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse WithdrawalRejected: %w", err)
	}
	wdID, err := uuid.Parse(j.WithdrawalID)
	if err != nil {
		return nil, fmt.Errorf("parse withdrawal_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.WithdrawalRejected{
		WithdrawalID: wdID,
		UserID:       userID,
		Asset:        j.Asset,
		Amount:       j.Amount,
		Sequence:     j.Sequence,
		Timestamp:    time.UnixMicro(j.TimestampUs),
		Reason:       j.Reason,
	}, nil
}

type markPriceJSON struct {
	Market         string `json:"market"`
	MarkPrice      int64  `json:"mark_price"`
	PriceSequence  int64  `json:"price_sequence"`
	PriceTimestamp int64  `json:"price_timestamp_us"`
	IndexPrice     int64  `json:"index_price"`
}

func parseMarkPriceUpdate(data []byte) (*event.MarkPriceUpdate, error) {
	var j markPriceJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse MarkPriceUpdate: %w", err)
	}
	return &event.MarkPriceUpdate{
		Market:         j.Market,
		MarkPrice:      j.MarkPrice,
		PriceSequence:  j.PriceSequence,
		PriceTimestamp: j.PriceTimestamp,
		IndexPrice:     j.IndexPrice,
	}, nil
}

type fundingSnapshotJSON struct {
	Market      string `json:"market"`
	FundingRate int64  `json:"funding_rate"`
	EpochID     int64  `json:"epoch_id"`
	MarkPrice   int64  `json:"mark_price"`
	EpochTs     int64  `json:"epoch_ts_us"`
}

func parseFundingRateSnapshot(data []byte) (*event.FundingRateSnapshot, error) {
	var j fundingSnapshotJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse FundingRateSnapshot: %w", err)
	}
	return &event.FundingRateSnapshot{
		Market:      j.Market,
		FundingRate: j.FundingRate,
		EpochID:     j.EpochID,
		MarkPrice:   j.MarkPrice,
		EpochTs:     j.EpochTs,
	}, nil
}

type fundingSettleJSON struct {
	Market  string `json:"market"`
	EpochID int64  `json:"epoch_id"`
}

func parseFundingEpochSettle(data []byte) (*event.FundingEpochSettle, error) {
	var j fundingSettleJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse FundingEpochSettle: %w", err)
	}
	return &event.FundingEpochSettle{
		Market:  j.Market,
		EpochID: j.EpochID,
	}, nil
}

type liquidationFillJSON struct {
	LiquidationID string `json:"liquidation_id"`
	FillID        string `json:"fill_id"`
	UserID        string `json:"user_id"`
	Market        string `json:"market"`
	Side          string `json:"side"`
	Quantity      int64  `json:"quantity"`
	Price         int64  `json:"price"`
	Fee           int64  `json:"fee"`
	Sequence      int64  `json:"sequence"`
	TimestampUs   int64  `json:"timestamp_us"`
}

func parseLiquidationFill(data []byte) (*event.LiquidationFill, error) {
	var j liquidationFillJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse LiquidationFill: %w", err)
	}
	liqID, err := uuid.Parse(j.LiquidationID)
	if err != nil {
		return nil, fmt.Errorf("parse liquidation_id: %w", err)
	}
	fillID, err := uuid.Parse(j.FillID)
	if err != nil {
		return nil, fmt.Errorf("parse fill_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}

	side := event.SideLong
	if j.Side == "short" {
		side = event.SideShort
	}

	return &event.LiquidationFill{
		LiquidationID: liqID,
		FillID:        fillID,
		UserID:        userID,
		Market:        j.Market,
		Side:          side,
		Quantity:      j.Quantity,
		Price:         j.Price,
		Fee:           j.Fee,
		Sequence:      j.Sequence,
		Timestamp:     j.TimestampUs,
	}, nil
}

type liquidationCompletedJSON struct {
	LiquidationID string `json:"liquidation_id"`
	UserID        string `json:"user_id"`
	Market        string `json:"market"`
	Deficit       int64  `json:"deficit"`
	Sequence      int64  `json:"sequence"`
	TimestampUs   int64  `json:"timestamp_us"`
}

func parseLiquidationCompleted(data []byte) (*event.LiquidationCompleted, error) {
	var j liquidationCompletedJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse LiquidationCompleted: %w", err)
	}
	liqID, err := uuid.Parse(j.LiquidationID)
	if err != nil {
		return nil, fmt.Errorf("parse liquidation_id: %w", err)
	}
	userID, err := uuid.Parse(j.UserID)
	if err != nil {
		return nil, fmt.Errorf("parse user_id: %w", err)
	}
	return &event.LiquidationCompleted{
		LiquidationID: liqID,
		UserID:        userID,
		Market:        j.Market,
		Deficit:       j.Deficit,
		Sequence:      j.Sequence,
		Timestamp:     j.TimestampUs,
	}, nil
}

type riskParamUpdateJSON struct {
	Market       string `json:"market"`
	IMFraction   int64  `json:"im_fraction"`
	MMFraction   int64  `json:"mm_fraction"`
	MaxLeverage  int64  `json:"max_leverage"`
	TickSize     int64  `json:"tick_size"`
	LotSize      int64  `json:"lot_size"`
	EffectiveSeq int64  `json:"effective_seq"`
	Sequence     int64  `json:"sequence"`
	TimestampUs  int64  `json:"timestamp_us"`
}

func parseRiskParamUpdate(data []byte) (*event.RiskParamUpdate, error) {
	var j riskParamUpdateJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("parse RiskParamUpdate: %w", err)
	}
	return &event.RiskParamUpdate{
		Market:       j.Market,
		IMFraction:   j.IMFraction,
		MMFraction:   j.MMFraction,
		MaxLeverage:  j.MaxLeverage,
		TickSize:     j.TickSize,
		LotSize:      j.LotSize,
		EffectiveSeq: j.EffectiveSeq,
		Sequence:     j.Sequence,
		Timestamp:    j.TimestampUs,
	}, nil
}
