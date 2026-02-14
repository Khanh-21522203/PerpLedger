package ingestion_test

import (
	"PerpLedger/internal/event"
	"PerpLedger/internal/ingestion"
	"encoding/json"
	"testing"
	"time"
)

func rawFromJSON(t *testing.T, v interface{}) ingestion.RawEvent {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return ingestion.RawEvent{
		Subject:   "test",
		Data:      data,
		Timestamp: time.Now(),
		AckFunc:   func() {},
		NakFunc:   func() {},
	}
}

func TestParseTradeFill(t *testing.T) {
	payload := map[string]interface{}{
		"fill_id":       "550e8400-e29b-41d4-a716-446655440000",
		"user_id":       "660e8400-e29b-41d4-a716-446655440001",
		"market":        "BTC-USDT-PERP",
		"side":          "long",
		"quantity":      int64(1_000_000),
		"price":         int64(50_000_00),
		"fee":           int64(5_000),
		"order_id":      "770e8400-e29b-41d4-a716-446655440002",
		"fill_sequence": int64(42),
		"timestamp_us":  int64(1700000000000000),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "TradeFill")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	tf, ok := evt.(*event.TradeFill)
	if !ok {
		t.Fatalf("expected *event.TradeFill, got %T", evt)
	}

	if tf.Market != "BTC-USDT-PERP" {
		t.Errorf("market: got %s, want BTC-USDT-PERP", tf.Market)
	}
	if tf.TradeSide != event.SideLong {
		t.Errorf("side: got %d, want SideLong", tf.TradeSide)
	}
	if tf.Quantity != 1_000_000 {
		t.Errorf("quantity: got %d, want 1_000_000", tf.Quantity)
	}
	if tf.Price != 50_000_00 {
		t.Errorf("price: got %d, want 50_000_00", tf.Price)
	}
	if tf.Fee != 5_000 {
		t.Errorf("fee: got %d, want 5_000", tf.Fee)
	}
	if tf.FillSequence != 42 {
		t.Errorf("fill_sequence: got %d, want 42", tf.FillSequence)
	}
	if tf.EventType() != event.EventTypeTradeFill {
		t.Errorf("event type: got %v, want TradeFill", tf.EventType())
	}
}

func TestParseDepositInitiated(t *testing.T) {
	payload := map[string]interface{}{
		"deposit_id":   "550e8400-e29b-41d4-a716-446655440000",
		"user_id":      "660e8400-e29b-41d4-a716-446655440001",
		"asset":        "USDT",
		"amount":       int64(1_000_000),
		"sequence":     int64(1),
		"timestamp_us": int64(1700000000000000),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "DepositInitiated")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	di, ok := evt.(*event.DepositInitiated)
	if !ok {
		t.Fatalf("expected *event.DepositInitiated, got %T", evt)
	}

	if di.Asset != "USDT" {
		t.Errorf("asset: got %s, want USDT", di.Asset)
	}
	if di.Amount != 1_000_000 {
		t.Errorf("amount: got %d, want 1_000_000", di.Amount)
	}
}

func TestParseDepositConfirmed(t *testing.T) {
	payload := map[string]interface{}{
		"deposit_id":   "550e8400-e29b-41d4-a716-446655440000",
		"user_id":      "660e8400-e29b-41d4-a716-446655440001",
		"asset":        "USDT",
		"amount":       int64(2_000_000),
		"sequence":     int64(2),
		"timestamp_us": int64(1700000000000000),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "DepositConfirmed")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	dc, ok := evt.(*event.DepositConfirmed)
	if !ok {
		t.Fatalf("expected *event.DepositConfirmed, got %T", evt)
	}

	if dc.Amount != 2_000_000 {
		t.Errorf("amount: got %d, want 2_000_000", dc.Amount)
	}
	if dc.EventType() != event.EventTypeDepositConfirmed {
		t.Errorf("event type: got %v, want DepositConfirmed", dc.EventType())
	}
}

func TestParseMarkPriceUpdate(t *testing.T) {
	payload := map[string]interface{}{
		"market":             "ETH-USDT-PERP",
		"mark_price":         int64(3_000_00),
		"price_sequence":     int64(100),
		"price_timestamp_us": int64(1700000000000000),
		"index_price":        int64(2_999_00),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "MarkPriceUpdate")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	mp, ok := evt.(*event.MarkPriceUpdate)
	if !ok {
		t.Fatalf("expected *event.MarkPriceUpdate, got %T", evt)
	}

	if mp.Market != "ETH-USDT-PERP" {
		t.Errorf("market: got %s, want ETH-USDT-PERP", mp.Market)
	}
	if mp.MarkPrice != 3_000_00 {
		t.Errorf("mark_price: got %d, want 3_000_00", mp.MarkPrice)
	}
	if mp.PriceSequence != 100 {
		t.Errorf("price_sequence: got %d, want 100", mp.PriceSequence)
	}
}

func TestParseFundingRateSnapshot(t *testing.T) {
	payload := map[string]interface{}{
		"market":       "BTC-USDT-PERP",
		"funding_rate": int64(100_000),
		"epoch_id":     int64(5),
		"mark_price":   int64(50_000_00),
		"epoch_ts_us":  int64(1700000000000000),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "FundingRateSnapshot")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	fs, ok := evt.(*event.FundingRateSnapshot)
	if !ok {
		t.Fatalf("expected *event.FundingRateSnapshot, got %T", evt)
	}

	if fs.EpochID != 5 {
		t.Errorf("epoch_id: got %d, want 5", fs.EpochID)
	}
	if fs.FundingRate != 100_000 {
		t.Errorf("funding_rate: got %d, want 100_000", fs.FundingRate)
	}
}

func TestParseRiskParamUpdate(t *testing.T) {
	payload := map[string]interface{}{
		"market":        "BTC-USDT-PERP",
		"im_fraction":   int64(100_000),
		"mm_fraction":   int64(50_000),
		"max_leverage":  int64(10),
		"tick_size":     int64(100),
		"lot_size":      int64(1_000),
		"effective_seq": int64(99),
		"sequence":      int64(1),
		"timestamp_us":  int64(1700000000000000),
	}

	raw := rawFromJSON(t, payload)
	evt, err := ingestion.ParseRawEvent(raw, "RiskParamUpdate")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	rp, ok := evt.(*event.RiskParamUpdate)
	if !ok {
		t.Fatalf("expected *event.RiskParamUpdate, got %T", evt)
	}

	if rp.Market != "BTC-USDT-PERP" {
		t.Errorf("market: got %s, want BTC-USDT-PERP", rp.Market)
	}
	if rp.IMFraction != 100_000 {
		t.Errorf("im_fraction: got %d, want 100_000", rp.IMFraction)
	}
	if rp.MMFraction != 50_000 {
		t.Errorf("mm_fraction: got %d, want 50_000", rp.MMFraction)
	}
}

func TestParseUnknownEventType_Fails(t *testing.T) {
	raw := ingestion.RawEvent{Data: []byte(`{}`)}
	_, err := ingestion.ParseRawEvent(raw, "NonExistentType")
	if err == nil {
		t.Fatal("expected error for unknown event type")
	}
}

func TestParseInvalidJSON_Fails(t *testing.T) {
	raw := ingestion.RawEvent{Data: []byte(`{invalid json`)}
	_, err := ingestion.ParseRawEvent(raw, "TradeFill")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseInvalidUUID_Fails(t *testing.T) {
	payload := map[string]interface{}{
		"fill_id":       "not-a-uuid",
		"user_id":       "also-not-a-uuid",
		"market":        "BTC-USDT-PERP",
		"side":          "long",
		"quantity":      int64(1),
		"price":         int64(1),
		"fee":           int64(0),
		"order_id":      "still-not-a-uuid",
		"fill_sequence": int64(0),
		"timestamp_us":  int64(0),
	}

	raw := rawFromJSON(t, payload)
	_, err := ingestion.ParseRawEvent(raw, "TradeFill")
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}
