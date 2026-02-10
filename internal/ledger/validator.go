package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// InvariantValidator checks ledger invariants
type InvariantValidator struct {
	tracker *BalanceTracker
}

func NewInvariantValidator(tracker *BalanceTracker) *InvariantValidator {
	return &InvariantValidator{
		tracker: tracker,
	}
}

// ValidateBatchBalance verifies batch is balanced (L-01)
func (v *InvariantValidator) ValidateBatchBalance(batch *Batch) error {
	return batch.Validate()
}

// ValidateFundingPoolZero verifies funding pool balance is zero after epoch (L-07)
func (v *InvariantValidator) ValidateFundingPoolZero(marketID string, assetID AssetID) error {
	key := NewSystemAccountKey(marketID, SubTypeSystemFundingPool, assetID)
	balance := v.tracker.GetBalance(key)

	if balance != 0 {
		return fmt.Errorf("funding pool for %s has non-zero balance: %d", marketID, balance)
	}

	return nil
}

// ValidateUserCollateralNonNegative checks user collateral >= 0 (L-09)
func (v *InvariantValidator) ValidateUserCollateralNonNegative(userID uuid.UUID, assetID AssetID) error {
	key := NewUserAccountKey(userID, SubTypeCollateral, assetID)
	return v.tracker.ValidateNonNegative(key)
}

// ValidateGlobalBalance verifies system is zero-sum (L-06)
func (v *InvariantValidator) ValidateGlobalBalance() error {
	totals := v.tracker.ComputeGlobalBalance()

	for assetID, total := range totals {
		if total != 0 {
			assetName, _ := GetAssetName(assetID)
			return fmt.Errorf("global balance for %s is non-zero: %d", assetName, total)
		}
	}

	return nil
}
