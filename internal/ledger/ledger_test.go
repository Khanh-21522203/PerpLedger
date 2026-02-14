package ledger_test

import (
	"PerpLedger/internal/ledger"
	"testing"

	"github.com/google/uuid"
)

// ============================================================================
// Test: AccountKey
// ============================================================================

func TestAccountKey_UserPath(t *testing.T) {
	userID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	assetID, _ := ledger.GetAssetID("USDT")
	key := ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID)

	path := key.AccountPath()
	expected := "user:550e8400-e29b-41d4-a716-446655440000:collateral:USDT"
	if path != expected {
		t.Errorf("got %q, want %q", path, expected)
	}
}

func TestAccountKey_SystemPath(t *testing.T) {
	assetID, _ := ledger.GetAssetID("USDT")
	key := ledger.NewSystemAccountKey("insurance", ledger.SubTypeSystemInsuranceFund, assetID)

	path := key.AccountPath()
	if path != "system:insurance_fund:USDT" {
		t.Errorf("got %q, want %q", path, "system:insurance_fund:USDT")
	}
}

func TestAccountKey_ExternalPath(t *testing.T) {
	assetID, _ := ledger.GetAssetID("USDT")
	key := ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID)

	path := key.AccountPath()
	if path != "external:deposits:USDT" {
		t.Errorf("got %q, want %q", path, "external:deposits:USDT")
	}
}

func TestGetAssetID_Known(t *testing.T) {
	id, ok := ledger.GetAssetID("USDT")
	if !ok {
		t.Fatal("USDT should be a known asset")
	}
	if id == 0 {
		t.Error("USDT asset ID should be non-zero")
	}
}

func TestGetAssetID_Unknown(t *testing.T) {
	_, ok := ledger.GetAssetID("DOGE")
	if ok {
		t.Error("DOGE should not be a known asset")
	}
}

// ============================================================================
// Test: BalanceTracker
// ============================================================================

func TestBalanceTracker_InitialBalanceZero(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	balance := bt.GetUserTotalBalance(userID, assetID)
	if balance != 0 {
		t.Errorf("initial balance should be 0, got %d", balance)
	}
}

func TestBalanceTracker_ApplyJournal(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	// Simulate deposit: debit user:collateral, credit external:deposits
	j := ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        1_000_000,
	}

	bt.ApplyJournal(j)

	collateral := bt.GetUserAvailableBalance(userID, assetID)
	if collateral != 1_000_000 {
		t.Errorf("collateral: got %d, want 1_000_000", collateral)
	}
}

func TestBalanceTracker_ApplyBatch(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")
	batchID := uuid.New()

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
				CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
				AssetID:       assetID,
				Amount:        500_000,
			},
		},
	}

	err := bt.ApplyBatch(batch)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	if bt.GetUserAvailableBalance(userID, assetID) != 500_000 {
		t.Errorf("expected 500_000 after batch apply")
	}
}

func TestBalanceTracker_GlobalBalanceZeroSum(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	// Deposit
	bt.ApplyJournal(ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        1_000_000,
	})

	// Transfer to reserved
	bt.ApplyJournal(ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeReserved, assetID),
		CreditAccount: ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		AssetID:       assetID,
		Amount:        300_000,
	})

	// Global balance should still be zero
	totals := bt.ComputeGlobalBalance()
	for aid, total := range totals {
		if total != 0 {
			t.Errorf("asset %d has non-zero global balance: %d", aid, total)
		}
	}
}

func TestBalanceTracker_ValidateSufficientAvailable(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	// No balance — should fail
	err := bt.ValidateSufficientAvailable(userID, assetID, 100)
	if err == nil {
		t.Error("expected error for insufficient balance")
	}

	// Add balance
	bt.ApplyJournal(ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        1_000,
	})

	// Now should pass
	err = bt.ValidateSufficientAvailable(userID, assetID, 1_000)
	if err != nil {
		t.Errorf("should have sufficient balance: %v", err)
	}

	// Asking for more should fail
	err = bt.ValidateSufficientAvailable(userID, assetID, 1_001)
	if err == nil {
		t.Error("expected error for 1_001 > 1_000")
	}
}

func TestBalanceTracker_Snapshot(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	bt.ApplyJournal(ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        999,
	})

	snap := bt.Snapshot()
	if len(snap) == 0 {
		t.Fatal("snapshot should not be empty")
	}

	// Mutating snapshot should not affect tracker
	for k := range snap {
		snap[k] = 0
	}

	if bt.GetUserAvailableBalance(userID, assetID) != 999 {
		t.Error("tracker balance should not be affected by snapshot mutation")
	}
}

// ============================================================================
// Test: Batch Validation
// ============================================================================

func TestBatchValidate_EmptyBatch_Fails(t *testing.T) {
	batch := &ledger.Batch{
		BatchID:  uuid.New(),
		Journals: []ledger.Journal{},
	}

	err := batch.Validate()
	if err == nil {
		t.Error("empty batch should fail validation")
	}
}

func TestBatchValidate_ZeroAmount_Fails(t *testing.T) {
	batchID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				DebitAccount:  ledger.NewUserAccountKey(uuid.New(), ledger.SubTypeCollateral, assetID),
				CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
				AssetID:       assetID,
				Amount:        0,
			},
		},
	}

	err := batch.Validate()
	if err == nil {
		t.Error("zero amount should fail validation")
	}
}

func TestBatchValidate_NegativeAmount_Fails(t *testing.T) {
	batchID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				DebitAccount:  ledger.NewUserAccountKey(uuid.New(), ledger.SubTypeCollateral, assetID),
				CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
				AssetID:       assetID,
				Amount:        -100,
			},
		},
	}

	err := batch.Validate()
	if err == nil {
		t.Error("negative amount should fail validation")
	}
}

func TestBatchValidate_SelfTransfer_Fails(t *testing.T) {
	batchID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")
	sameAccount := ledger.NewUserAccountKey(uuid.New(), ledger.SubTypeCollateral, assetID)

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				DebitAccount:  sameAccount,
				CreditAccount: sameAccount,
				AssetID:       assetID,
				Amount:        100,
			},
		},
	}

	err := batch.Validate()
	if err == nil {
		t.Error("self-transfer should fail validation")
	}
}

func TestBatchValidate_MismatchedBatchID_Fails(t *testing.T) {
	batchID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       uuid.New(), // Different batch ID
				DebitAccount:  ledger.NewUserAccountKey(uuid.New(), ledger.SubTypeCollateral, assetID),
				CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
				AssetID:       assetID,
				Amount:        100,
			},
		},
	}

	err := batch.Validate()
	if err == nil {
		t.Error("mismatched batch ID should fail validation")
	}
}

func TestBatchValidate_ValidBatch_Passes(t *testing.T) {
	batchID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")

	batch := &ledger.Batch{
		BatchID: batchID,
		Journals: []ledger.Journal{
			{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				DebitAccount:  ledger.NewUserAccountKey(uuid.New(), ledger.SubTypeCollateral, assetID),
				CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
				AssetID:       assetID,
				Amount:        1_000_000,
			},
		},
	}

	err := batch.Validate()
	if err != nil {
		t.Errorf("valid batch should pass: %v", err)
	}
}

// ============================================================================
// Test: InvariantValidator
// ============================================================================

func TestInvariantValidator_GlobalBalanceZero(t *testing.T) {
	bt := ledger.NewBalanceTracker()
	v := ledger.NewInvariantValidator(bt)

	// Empty ledger — should pass
	err := v.ValidateGlobalBalance()
	if err != nil {
		t.Errorf("empty ledger should have zero global balance: %v", err)
	}

	// Add balanced journal
	userID := uuid.New()
	assetID, _ := ledger.GetAssetID("USDT")
	bt.ApplyJournal(ledger.Journal{
		JournalID:     uuid.New(),
		BatchID:       uuid.New(),
		DebitAccount:  ledger.NewUserAccountKey(userID, ledger.SubTypeCollateral, assetID),
		CreditAccount: ledger.NewExternalAccountKey(ledger.SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        1_000_000,
	})

	// Still zero-sum
	err = v.ValidateGlobalBalance()
	if err != nil {
		t.Errorf("balanced ledger should have zero global balance: %v", err)
	}
}
