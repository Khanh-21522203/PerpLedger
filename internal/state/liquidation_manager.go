// internal/state/liquidation_manager.go (NEW)
package state

import (
	"fmt"
	"github.com/google/uuid"
)

// LiquidationManager tracks active liquidations
type LiquidationManager struct {
	activeLiquidations map[uuid.UUID]*ActiveLiquidation // liquidation_id -> liquidation
	positionManager    *PositionManager
}

type ActiveLiquidation struct {
	LiquidationID uuid.UUID
	UserID        uuid.UUID
	MarketID      string
	TriggeredAt   int64 // Sequence when triggered
	InitialSize   int64
	RemainingSize int64
	State         LiquidationState
}

func NewLiquidationManager(pm *PositionManager) *LiquidationManager {
	return &LiquidationManager{
		activeLiquidations: make(map[uuid.UUID]*ActiveLiquidation),
		positionManager:    pm,
	}
}

// TriggerLiquidation creates a new liquidation
func (lm *LiquidationManager) TriggerLiquidation(
	userID uuid.UUID,
	marketID string,
	sequence int64,
) (uuid.UUID, error) {
	pos := lm.positionManager.GetPosition(userID, marketID)
	if pos == nil || pos.IsFlat() {
		return uuid.Nil, fmt.Errorf("no position to liquidate")
	}

	// Check current state allows triggering
	if pos.LiquidationState != LiquidationStateHealthy &&
		pos.LiquidationState != LiquidationStateAtRisk {
		return uuid.Nil, fmt.Errorf("position already in liquidation")
	}

	liquidationID := uuid.New()

	lm.activeLiquidations[liquidationID] = &ActiveLiquidation{
		LiquidationID: liquidationID,
		UserID:        userID,
		MarketID:      marketID,
		TriggeredAt:   sequence,
		InitialSize:   pos.Size,
		RemainingSize: pos.Size,
		State:         LiquidationStateAtRisk,
	}

	// Update position state
	pos.LiquidationState = LiquidationStateAtRisk
	pos.LiquidationID = &liquidationID
	pos.Version++

	return liquidationID, nil
}

// ProcessLiquidationFill handles a liquidation fill
func (lm *LiquidationManager) ProcessLiquidationFill(
	liquidationID uuid.UUID,
	fillQuantity int64,
) error {
	liq, ok := lm.activeLiquidations[liquidationID]
	if !ok {
		return fmt.Errorf("unknown liquidation_id: %s", liquidationID)
	}

	pos := lm.positionManager.GetPosition(liq.UserID, liq.MarketID)
	if pos == nil {
		return fmt.Errorf("position not found")
	}

	// Transition to InLiquidation on first fill
	if liq.State == LiquidationStateAtRisk {
		if !pos.LiquidationState.CanTransitionTo(LiquidationStateInLiquidation) {
			return fmt.Errorf("invalid state transition: %s -> InLiquidation",
				pos.LiquidationState)
		}
		liq.State = LiquidationStateInLiquidation
		pos.LiquidationState = LiquidationStateInLiquidation
	}

	// Update remaining size
	liq.RemainingSize -= fillQuantity

	if liq.RemainingSize < 0 {
		return fmt.Errorf("liquidation overfilled")
	}

	// Check if fully liquidated
	if liq.RemainingSize == 0 {
		if !pos.LiquidationState.CanTransitionTo(LiquidationStateClosed) {
			return fmt.Errorf("invalid state transition: %s -> Closed",
				pos.LiquidationState)
		}
		liq.State = LiquidationStateClosed
		pos.LiquidationState = LiquidationStateClosed
	} else {
		// Partial liquidation
		if !pos.LiquidationState.CanTransitionTo(LiquidationStatePartiallyLiquidated) {
			return fmt.Errorf("invalid state transition: %s -> PartiallyLiquidated",
				pos.LiquidationState)
		}
		liq.State = LiquidationStatePartiallyLiquidated
		pos.LiquidationState = LiquidationStatePartiallyLiquidated
	}

	pos.Version++

	return nil
}

// CheckMarginRecovery checks if liquidation can be stopped
func (lm *LiquidationManager) CheckMarginRecovery(
	liquidationID uuid.UUID,
	marginHealthy bool,
) error {
	liq, ok := lm.activeLiquidations[liquidationID]
	if !ok {
		return nil // Already completed
	}

	if !marginHealthy {
		return nil // Still unhealthy
	}

	// Margin recovered - stop liquidation
	pos := lm.positionManager.GetPosition(liq.UserID, liq.MarketID)
	if pos == nil {
		return fmt.Errorf("position not found")
	}

	if !pos.LiquidationState.CanTransitionTo(LiquidationStateHealthy) {
		return fmt.Errorf("invalid state transition: %s -> Healthy",
			pos.LiquidationState)
	}

	pos.LiquidationState = LiquidationStateHealthy
	pos.LiquidationID = nil
	pos.Version++

	// Remove from active liquidations
	delete(lm.activeLiquidations, liquidationID)

	return nil
}

// GetActiveLiquidation retrieves active liquidation info
func (lm *LiquidationManager) GetActiveLiquidation(liquidationID uuid.UUID) (*ActiveLiquidation, bool) {
	liq, ok := lm.activeLiquidations[liquidationID]
	return liq, ok
}
