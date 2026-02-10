package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// JournalType represents the purpose of a journal entry
type JournalType int32

const (
	JournalTypeDeposit JournalType = iota
	JournalTypeDepositPending
	JournalTypeDepositConfirm
	JournalTypeWithdrawal
	JournalTypeWithdrawalPending
	JournalTypeWithdrawalConfirm
	JournalTypeWithdrawalReject
	JournalTypeTradePnL
	JournalTypeTradeFee
	JournalTypeMarginReserve
	JournalTypeMarginRelease
	JournalTypeFundingAccrual
	JournalTypeFundingSettle
	JournalTypeLiquidationTransfer
	JournalTypeInsuranceFundDebit
	JournalTypeAdjustment
)

// Journal represents a single double-entry journal entry
type Journal struct {
	JournalID     uuid.UUID   // Unique identifier
	BatchID       uuid.UUID   // Groups balanced entries
	EventRef      string      // Idempotency key of source event
	Sequence      int64       // Global event sequence
	DebitAccount  AccountKey  // Account receiving debit (balance increases)
	CreditAccount AccountKey  // Account receiving credit (balance decreases)
	AssetID       AssetID     // Asset being transferred
	Amount        int64       // Fixed-point amount (ALWAYS positive)
	JournalType   JournalType // Entry type
	Timestamp     int64       // Versioned input timestamp (epoch microseconds)
}

// Batch represents a balanced set of journal entries
type Batch struct {
	BatchID   uuid.UUID
	EventRef  string
	Sequence  int64
	Timestamp int64
	Journals  []Journal
}

// Validate ensures the batch is balanced
func (b *Batch) Validate() error {
	if len(b.Journals) == 0 {
		return fmt.Errorf("batch %s is empty", b.BatchID)
	}

	// Sum debits and credits per asset
	debitSums := make(map[AssetID]int64)
	creditSums := make(map[AssetID]int64)

	for _, j := range b.Journals {
		// Validate amount is positive
		if j.Amount <= 0 {
			return fmt.Errorf("journal %s has non-positive amount: %d", j.JournalID, j.Amount)
		}

		// Validate batch consistency
		if j.BatchID != b.BatchID {
			return fmt.Errorf("journal %s has mismatched batch_id", j.JournalID)
		}

		// Accumulate sums
		debitSums[j.AssetID] += j.Amount
		creditSums[j.AssetID] += j.Amount
	}

	// Verify balance per asset
	for assetID, debitSum := range debitSums {
		creditSum := creditSums[assetID]
		if debitSum != creditSum {
			assetName, _ := GetAssetName(assetID)
			return fmt.Errorf("batch %s unbalanced for %s: debits=%d credits=%d",
				b.BatchID, assetName, debitSum, creditSum)
		}
	}

	return nil
}
