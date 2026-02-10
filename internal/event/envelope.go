package event

import (
	"time"
)

// EventType discriminator for event payloads
type EventType int32

const (
	EventTypeUnknown EventType = iota
	EventTypeTradeFill
	EventTypeDepositInitiated
	EventTypeDepositConfirmed
	EventTypeWithdrawalRequested
	EventTypeWithdrawalConfirmed
	EventTypeWithdrawalRejected
	EventTypeMarkPriceUpdate
	EventTypeFundingRateSnapshot
	EventTypeFundingEpochSettle
	EventTypeRiskParamUpdate
	EventTypeLiquidationTriggered
	EventTypeLiquidationFill
	EventTypeLiquidationCompleted
)

// EventEnvelope wraps every event in the log
type EventEnvelope struct {
	// Global monotonic sequence assigned by core
	Sequence int64

	// Stable idempotency key from upstream
	IdempotencyKey string

	// Event type discriminator
	EventType EventType

	// Market context (nullable for global events)
	MarketID *string

	// Versioned input timestamp (NOT wall-clock)
	Timestamp time.Time

	// Upstream sequence for ordering validation
	SourceSequence int64

	// Protobuf-encoded event-specific data
	Payload []byte

	// SHA-256 of state AFTER applying this event
	StateHash [32]byte

	// Previous event's state hash (chain integrity)
	PrevHash [32]byte
}

// Event is the interface all event payloads must implement
type Event interface {
	// IdempotencyKey returns the stable dedup key
	IdempotencyKey() string

	// EventType returns the discriminator
	EventType() EventType

	// MarketID returns the market context (nil for global events)
	MarketID() *string

	// SourceSequence returns upstream ordering key
	SourceSequence() int64
}

func (et EventType) String() string {
	switch et {
	case EventTypeTradeFill:
		return "TradeFill"
	case EventTypeDepositInitiated:
		return "DepositInitiated"
	case EventTypeDepositConfirmed:
		return "DepositConfirmed"
	case EventTypeWithdrawalRequested:
		return "WithdrawalRequested"
	case EventTypeWithdrawalConfirmed:
		return "WithdrawalConfirmed"
	case EventTypeWithdrawalRejected:
		return "WithdrawalRejected"
	case EventTypeMarkPriceUpdate:
		return "MarkPriceUpdate"
	case EventTypeFundingRateSnapshot:
		return "FundingRateSnapshot"
	case EventTypeFundingEpochSettle:
		return "FundingEpochSettle"
	case EventTypeLiquidationTriggered:
		return "LiquidationTriggered"
	case EventTypeLiquidationFill:
		return "LiquidationFill"
	case EventTypeLiquidationCompleted:
		return "LiquidationCompleted"
	case EventTypeRiskParamUpdate:
		return "RiskParamUpdate"
	default:
		return "Unknown"
	}
}
