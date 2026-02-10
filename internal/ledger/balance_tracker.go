package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// BalanceTracker maintains in-memory account balances
type BalanceTracker struct {
	balances map[AccountKey]int64
}

func NewBalanceTracker() *BalanceTracker {
	return &BalanceTracker{
		balances: make(map[AccountKey]int64),
	}
}

// ApplyJournal applies a single journal entry to balances
func (bt *BalanceTracker) ApplyJournal(j Journal) {
	bt.balances[j.DebitAccount] += j.Amount
	bt.balances[j.CreditAccount] -= j.Amount
}

// ApplyBatch applies all journals in a batch
func (bt *BalanceTracker) ApplyBatch(batch *Batch) error {
	if err := batch.Validate(); err != nil {
		return fmt.Errorf("invalid batch: %w", err)
	}

	for _, j := range batch.Journals {
		bt.ApplyJournal(j)
	}

	return nil
}

// GetBalance returns the current balance for an account
func (bt *BalanceTracker) GetBalance(key AccountKey) int64 {
	return bt.balances[key]
}

// === User Balance Queries (BS-03: total_balance = available + reserved) ===

// GetUserTotalBalance returns total balance (collateral + reserved)
func (bt *BalanceTracker) GetUserTotalBalance(userID uuid.UUID, assetID AssetID) int64 {
	collateral := bt.GetBalance(NewUserAccountKey(userID, SubTypeCollateral, assetID))
	reserved := bt.GetBalance(NewUserAccountKey(userID, SubTypeReserved, assetID))
	return collateral + reserved
}

// GetUserAvailableBalance returns available balance (collateral only)
// This is used for withdrawal checks and new position margin checks (BS-07)
func (bt *BalanceTracker) GetUserAvailableBalance(userID uuid.UUID, assetID AssetID) int64 {
	return bt.GetBalance(NewUserAccountKey(userID, SubTypeCollateral, assetID))
}

// GetUserReservedBalance returns reserved (margin-locked) balance
func (bt *BalanceTracker) GetUserReservedBalance(userID uuid.UUID, assetID AssetID) int64 {
	return bt.GetBalance(NewUserAccountKey(userID, SubTypeReserved, assetID))
}

// GetUserPendingDeposit returns unconfirmed deposit amount
func (bt *BalanceTracker) GetUserPendingDeposit(userID uuid.UUID, assetID AssetID) int64 {
	return bt.GetBalance(NewUserAccountKey(userID, SubTypePendingDeposit, assetID))
}

// GetUserPendingWithdrawal returns pending withdrawal amount
func (bt *BalanceTracker) GetUserPendingWithdrawal(userID uuid.UUID, assetID AssetID) int64 {
	return bt.GetBalance(NewUserAccountKey(userID, SubTypePendingWithdrawal, assetID))
}

// GetUserFundingAccrual returns unsettled funding for a market
func (bt *BalanceTracker) GetUserFundingAccrual(userID uuid.UUID, marketID string, assetID AssetID) int64 {
	// Special handling: funding_accrual uses market_id in entity field
	var entityID [16]byte
	copy(entityID[:], []byte(marketID))

	key := AccountKey{
		Scope:    AccountScopeUser,
		EntityID: userID,
		SubType:  SubTypeFundingAccrual,
		AssetID:  assetID,
	}
	return bt.GetBalance(key)
}

// === Invariant Checks ===

// ValidateAvailableNonNegative checks available_balance >= 0 (BS-01)
func (bt *BalanceTracker) ValidateAvailableNonNegative(userID uuid.UUID, assetID AssetID) error {
	available := bt.GetUserAvailableBalance(userID, assetID)
	if available < 0 {
		return fmt.Errorf("user %s has negative available balance for asset %d: %d",
			userID.String(), assetID, available)
	}
	return nil
}

// ValidateReservedNonNegative checks reserved_balance >= 0 (BS-02)
func (bt *BalanceTracker) ValidateReservedNonNegative(userID uuid.UUID, assetID AssetID) error {
	reserved := bt.GetUserReservedBalance(userID, assetID)
	if reserved < 0 {
		return fmt.Errorf("user %s has negative reserved balance for asset %d: %d",
			userID.String(), assetID, reserved)
	}
	return nil
}

// ValidateSufficientAvailable checks if user has enough available balance
func (bt *BalanceTracker) ValidateSufficientAvailable(userID uuid.UUID, assetID AssetID, required int64) error {
	available := bt.GetUserAvailableBalance(userID, assetID)
	if available < required {
		return fmt.Errorf("insufficient available balance: have=%d, need=%d", available, required)
	}
	return nil
}

// ValidateSufficientReserved checks if user has enough reserved to release
func (bt *BalanceTracker) ValidateSufficientReserved(userID uuid.UUID, assetID AssetID, required int64) error {
	reserved := bt.GetUserReservedBalance(userID, assetID)
	if reserved < required {
		return fmt.Errorf("insufficient reserved balance: have=%d, need=%d", reserved, required)
	}
	return nil
}

// ComputeGlobalBalance sums all account balances (should be 0 for zero-sum ledger)
func (bt *BalanceTracker) ComputeGlobalBalance() map[AssetID]int64 {
	totals := make(map[AssetID]int64)

	for key, balance := range bt.balances {
		totals[key.AssetID] += balance
	}

	return totals
}

// ValidateNonNegative checks that a specific account balance is >= 0
func (bt *BalanceTracker) ValidateNonNegative(key AccountKey) error {
	balance := bt.GetBalance(key)
	if balance < 0 {
		return fmt.Errorf("account %s has negative balance: %d", key.AccountPath(), balance)
	}
	return nil
}

// Snapshot returns a copy of all balances (for state hashing)
func (bt *BalanceTracker) Snapshot() map[AccountKey]int64 {
	snapshot := make(map[AccountKey]int64, len(bt.balances))
	for k, v := range bt.balances {
		snapshot[k] = v
	}
	return snapshot
}
