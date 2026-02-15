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

// Validate ensures the batch is well-formed.
// Per flow deterministic-core-event-loop-flowchart: "Verify journal batch balanced:
// sum debits == sum credits". Each journal entry transfers Amount from CreditAccount
// to DebitAccount. We verify per-entry invariants AND that the batch is globally
// balanced per asset (net flow across all accounts sums to zero).
func (b *Batch) Validate() error {
	if len(b.Journals) == 0 {
		return fmt.Errorf("batch %s is empty", b.BatchID)
	}

	// Per-asset balance check: track net flow per (account, asset)
	// Debit increases balance (+Amount), Credit decreases balance (-Amount)
	type accountAsset struct {
		Account AccountKey
		Asset   AssetID
	}
	netFlows := make(map[accountAsset]int64)

	for _, j := range b.Journals {
		// Validate amount is positive (L-02)
		if j.Amount <= 0 {
			return fmt.Errorf("journal %s has non-positive amount: %d", j.JournalID, j.Amount)
		}

		// Validate batch consistency
		if j.BatchID != b.BatchID {
			return fmt.Errorf("journal %s has mismatched batch_id", j.JournalID)
		}

		// Validate debit != credit (no self-transfers)
		if j.DebitAccount == j.CreditAccount {
			return fmt.Errorf("journal %s has same debit and credit account", j.JournalID)
		}

		// Accumulate net flows per account per asset
		netFlows[accountAsset{j.DebitAccount, j.AssetID}] += j.Amount
		netFlows[accountAsset{j.CreditAccount, j.AssetID}] -= j.Amount
	}

	// Verify global balance: sum of all flows per asset must be zero
	assetTotals := make(map[AssetID]int64)
	for key, flow := range netFlows {
		assetTotals[key.Asset] += flow
	}
	for assetID, total := range assetTotals {
		if total != 0 {
			return fmt.Errorf("batch %s is unbalanced for asset %d: net=%d", b.BatchID, assetID, total)
		}
	}

	return nil
}
