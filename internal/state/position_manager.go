package state

import (
	"PerpLedger/internal/event"
	fpmath "PerpLedger/internal/math"
	"fmt"

	"github.com/google/uuid"
)

// PositionManager manages position state and PnL calculations
type PositionManager struct {
	positions  map[PositionKey]*Position
	markPrices map[string]*MarkPriceState
}

type PositionKey struct {
	UserID   uuid.UUID
	MarketID string
}

// MarkPriceState tracks latest mark price per market
type MarkPriceState struct {
	Price         int64
	PriceSequence int64
	Timestamp     int64
}

func NewPositionManager() *PositionManager {
	return &PositionManager{
		positions:  make(map[PositionKey]*Position),
		markPrices: make(map[string]*MarkPriceState),
	}
}

// GetPosition returns existing position or nil
func (pm *PositionManager) GetPosition(userID uuid.UUID, marketID string) *Position {
	key := PositionKey{UserID: userID, MarketID: marketID}
	return pm.positions[key]
}

// GetOrCreatePosition returns existing or creates new flat position
func (pm *PositionManager) GetOrCreatePosition(userID uuid.UUID, marketID string) *Position {
	key := PositionKey{UserID: userID, MarketID: marketID}
	pos := pm.positions[key]

	if pos == nil {
		pos = &Position{
			UserID:           userID,
			MarketID:         marketID,
			Side:             event.SideFlat,
			Size:             0,
			AvgEntryPrice:    0,
			RealizedPnL:      0,
			LastFundingEpoch: 0,
			LiquidationState: LiquidationStateHealthy,
			Version:          0,
		}
		pm.positions[key] = pos
	}

	return pos
}

// UpdateMarkPrice processes a mark price update
func (pm *PositionManager) UpdateMarkPrice(marketID string, price int64, sequence int64, timestamp int64) error {
	current := pm.markPrices[marketID]

	if current != nil {
		// Check sequence ordering
		if sequence <= current.PriceSequence {
			// Stale or duplicate - silently ignore (idempotent)
			return nil
		}

		if sequence > current.PriceSequence+1 {
			// Gap detected - log warning but accept
			// (price gaps are tolerable unlike fill gaps)
		}
	}

	pm.markPrices[marketID] = &MarkPriceState{
		Price:         price,
		PriceSequence: sequence,
		Timestamp:     timestamp,
	}

	return nil
}

// GetMarkPrice returns current mark price for a market
func (pm *PositionManager) GetMarkPrice(marketID string) (int64, bool) {
	state := pm.markPrices[marketID]
	if state == nil {
		return 0, false
	}
	return state.Price, true
}

// ComputeUnrealizedPnL calculates unrealized PnL for a position (derived value)
func (pm *PositionManager) ComputeUnrealizedPnL(pos *Position) (int64, error) {
	if pos.IsFlat() {
		return 0, nil
	}

	markPrice, ok := pm.GetMarkPrice(pos.MarketID)
	if !ok {
		return 0, fmt.Errorf("no mark price for market %s", pos.MarketID)
	}

	return fpmath.ComputeUnrealizedPnL(
		pos.SideSign(),
		markPrice,
		pos.AvgEntryPrice,
		pos.Size,
		fpmath.PriceConfig.Scale,
		fpmath.QuantityConfig.Scale,
		fpmath.QuoteConfig.Scale,
	), nil
}

// ComputeTotalUnrealizedPnL sums unrealized PnL across all user positions
func (pm *PositionManager) ComputeTotalUnrealizedPnL(userID uuid.UUID) int64 {
	var total int64

	for key, pos := range pm.positions {
		if key.UserID == userID && !pos.IsFlat() {
			upnl, err := pm.ComputeUnrealizedPnL(pos)
			if err == nil {
				total += upnl
			}
		}
	}

	return total
}

// ComputePositionNotional calculates position notional value
func (pm *PositionManager) ComputePositionNotional(pos *Position) (int64, error) {
	if pos.IsFlat() {
		return 0, nil
	}

	markPrice, ok := pm.GetMarkPrice(pos.MarketID)
	if !ok {
		return 0, fmt.Errorf("no mark price for market %s", pos.MarketID)
	}

	return fpmath.ComputeNotional(
		pos.Size,
		markPrice,
		fpmath.PriceConfig.Scale,
		fpmath.QuantityConfig.Scale,
		fpmath.QuoteConfig.Scale,
	), nil
}

// ComputeTotalNotional sums notional across all user positions
func (pm *PositionManager) ComputeTotalNotional(userID uuid.UUID) int64 {
	var total int64

	for key, pos := range pm.positions {
		if key.UserID == userID && !pos.IsFlat() {
			notional, err := pm.ComputePositionNotional(pos)
			if err == nil {
				total += notional
			}
		}
	}

	return total
}

// ApplyTradeFill updates position state from a trade fill
// Returns: (realized_pnl, margin_reserve_delta, margin_release_delta)
func (pm *PositionManager) ApplyTradeFill(
	userID uuid.UUID,
	marketID string,
	side event.Side,
	quantity int64,
	price int64,
) (realizedPnL int64, marginReserveDelta int64, marginReleaseDelta int64) {
	pos := pm.GetOrCreatePosition(userID, marketID)

	// Case 1: Flat position -> open new
	if pos.IsFlat() {
		pos.Side = side
		pos.Size = quantity
		pos.AvgEntryPrice = price
		pos.Version++

		// Calculate margin reserve (will be computed by caller with IM%)
		return 0, 0, 0
	}

	// Case 2: Same side -> increase position
	if pos.Side == side {
		newAvgEntry := fpmath.ComputeAvgEntryPrice(
			pos.Size,
			pos.AvgEntryPrice,
			quantity,
			price,
		)

		pos.Size += quantity
		pos.AvgEntryPrice = newAvgEntry
		pos.Version++

		return 0, 0, 0
	}

	// Case 3: Opposite side -> reduce, close, or flip
	if quantity < pos.Size {
		// Partial close
		realizedPnL = pm.computeRealizedPnLForClose(pos, quantity, price)

		// Calculate proportional margin release
		marginReleaseDelta = 0 // Will be computed by caller based on reserved balance

		pos.Size -= quantity
		pos.RealizedPnL += realizedPnL
		pos.Version++

		return realizedPnL, 0, marginReleaseDelta

	} else if quantity == pos.Size {
		// Full close
		realizedPnL = pm.computeRealizedPnLForClose(pos, quantity, price)

		// Release all margin
		marginReleaseDelta = 0 // Will be computed by caller

		pos.Side = event.SideFlat
		pos.Size = 0
		pos.AvgEntryPrice = 0
		pos.RealizedPnL += realizedPnL
		pos.Version++

		return realizedPnL, 0, marginReleaseDelta

	} else {
		// Close and flip
		// Step 1: Close existing position
		closePnL := pm.computeRealizedPnLForClose(pos, pos.Size, price)

		// Step 2: Open new position on opposite side
		remainingQty := quantity - pos.Size

		pos.Side = side
		pos.Size = remainingQty
		pos.AvgEntryPrice = price
		pos.RealizedPnL += closePnL
		pos.Version++

		return closePnL, 0, 0
	}
}

func (pm *PositionManager) computeRealizedPnLForClose(pos *Position, closedQty int64, exitPrice int64) int64 {
	return fpmath.ComputeRealizedPnL(
		pos.SideSign(),
		exitPrice,
		pos.AvgEntryPrice,
		closedQty,
		fpmath.PriceConfig.Scale,
		fpmath.QuantityConfig.Scale,
		fpmath.QuoteConfig.Scale,
	)
}

// SetPosition directly sets a position (used for snapshot restore)
func (pm *PositionManager) SetPosition(pos *Position) {
	key := PositionKey{UserID: pos.UserID, MarketID: pos.MarketID}
	pm.positions[key] = pos
}

// RestoreMarkPrice directly sets a mark price (used for snapshot restore)
func (pm *PositionManager) RestoreMarkPrice(marketID string, mp *MarkPriceState) {
	pm.markPrices[marketID] = mp
}

// GetAllMarkPrices returns all mark prices (for snapshot creation)
func (pm *PositionManager) GetAllMarkPrices() map[string]*MarkPriceState {
	result := make(map[string]*MarkPriceState, len(pm.markPrices))
	for k, v := range pm.markPrices {
		result[k] = v
	}
	return result
}

// GetAllPositions returns all positions (for iteration)
func (pm *PositionManager) GetAllPositions() []*Position {
	result := make([]*Position, 0, len(pm.positions))
	for _, pos := range pm.positions {
		result = append(result, pos)
	}
	return result
}

// GetUserPositions returns all positions for a user
func (pm *PositionManager) GetUserPositions(userID uuid.UUID) []*Position {
	result := make([]*Position, 0)
	for key, pos := range pm.positions {
		if key.UserID == userID {
			result = append(result, pos)
		}
	}
	return result
}
