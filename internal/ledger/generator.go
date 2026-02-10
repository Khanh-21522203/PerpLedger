package ledger

import (
	"PerpLedger/internal/event"
	fpmath "PerpLedger/internal/math"
	"fmt"

	"github.com/google/uuid"
)

// JournalGenerator creates balanced journal batches from events
type JournalGenerator struct {
	sequence       int64
	balanceTracker *BalanceTracker // Add reference for pre-checks
}

func NewJournalGenerator(startSequence int64, tracker *BalanceTracker) *JournalGenerator {
	return &JournalGenerator{
		sequence:       startSequence,
		balanceTracker: tracker,
	}
}

// GenerateDepositInitiated creates journals for a pending deposit.
// Moves funds: external:deposits → user:pending_deposit
func (jg *JournalGenerator) GenerateDepositInitiated(
	evt *event.DepositInitiated,
	assetID AssetID,
) (*Batch, error) {
	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  evt.DepositID.String(),
		Sequence:  jg.sequence,
		Timestamp: evt.Timestamp.UnixMicro(),
		Journals:  make([]Journal, 0, 1),
	}

	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      evt.DepositID.String(),
		Sequence:      jg.sequence,
		DebitAccount:  NewUserAccountKey(evt.UserID, SubTypePendingDeposit, assetID),
		CreditAccount: NewExternalAccountKey(SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        evt.Amount,
		JournalType:   JournalTypeDepositPending,
		Timestamp:     evt.Timestamp.UnixMicro(),
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// GenerateDepositConfirmed creates journals for a confirmed deposit.
// Moves funds: external:deposits → user:collateral
// (If a pending deposit exists, it should be cleared separately.)
func (jg *JournalGenerator) GenerateDepositConfirmed(
	evt *event.DepositConfirmed,
	assetID AssetID,
) (*Batch, error) {
	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  evt.DepositID.String(),
		Sequence:  jg.sequence,
		Timestamp: evt.Timestamp.UnixMicro(),
		Journals:  make([]Journal, 0, 1),
	}

	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      evt.DepositID.String(),
		Sequence:      jg.sequence,
		DebitAccount:  NewUserAccountKey(evt.UserID, SubTypeCollateral, assetID),
		CreditAccount: NewExternalAccountKey(SubTypeExternalDeposits, assetID),
		AssetID:       assetID,
		Amount:        evt.Amount,
		JournalType:   JournalTypeDepositConfirm,
		Timestamp:     evt.Timestamp.UnixMicro(),
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// GenerateWithdrawalRequested creates journals for withdrawal request.
// Pre-check: user must have sufficient available balance (BS-01, BS-07).
func (jg *JournalGenerator) GenerateWithdrawalRequested(
	userID uuid.UUID,
	withdrawalID uuid.UUID,
	amount int64,
	assetID AssetID,
	timestamp int64,
) (*Batch, error) {
	// PRE-CHECK: Validate sufficient available balance (BS-07)
	if err := jg.balanceTracker.ValidateSufficientAvailable(userID, assetID, amount); err != nil {
		return nil, fmt.Errorf("withdrawal pre-check failed: %w", err)
	}

	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  withdrawalID.String(),
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	// Lock funds: user:collateral -> user:pending_withdrawal
	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      withdrawalID.String(),
		Sequence:      jg.sequence,
		DebitAccount:  NewUserAccountKey(userID, SubTypePendingWithdrawal, assetID),
		CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, assetID),
		AssetID:       assetID,
		Amount:        amount,
		JournalType:   JournalTypeWithdrawalPending,
		Timestamp:     timestamp,
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// GenerateWithdrawalConfirmed finalizes withdrawal (clears pending)
func (jg *JournalGenerator) GenerateWithdrawalConfirmed(
	userID uuid.UUID,
	withdrawalID uuid.UUID,
	amount int64,
	assetID AssetID,
	timestamp int64,
) (*Batch, error) {
	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  withdrawalID.String(),
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	// Finalize: user:pending_withdrawal -> external:withdrawals
	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      withdrawalID.String(),
		Sequence:      jg.sequence,
		DebitAccount:  NewExternalAccountKey(SubTypeExternalWithdrawals, assetID),
		CreditAccount: NewUserAccountKey(userID, SubTypePendingWithdrawal, assetID),
		AssetID:       assetID,
		Amount:        amount,
		JournalType:   JournalTypeWithdrawalConfirm,
		Timestamp:     timestamp,
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// GenerateWithdrawalRejected reverses pending withdrawal
func (jg *JournalGenerator) GenerateWithdrawalRejected(
	userID uuid.UUID,
	withdrawalID uuid.UUID,
	amount int64,
	assetID AssetID,
	timestamp int64,
) (*Batch, error) {
	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  withdrawalID.String(),
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	// Reverse: user:pending_withdrawal -> user:collateral
	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      withdrawalID.String(),
		Sequence:      jg.sequence,
		DebitAccount:  NewUserAccountKey(userID, SubTypeCollateral, assetID),
		CreditAccount: NewUserAccountKey(userID, SubTypePendingWithdrawal, assetID),
		AssetID:       assetID,
		Amount:        amount,
		JournalType:   JournalTypeWithdrawalReject,
		Timestamp:     timestamp,
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// GenerateTradeFill creates journals for a trade fill
// Pre-check: user must have sufficient available balance for fee + margin
func (jg *JournalGenerator) GenerateTradeFill(
	userID uuid.UUID,
	fillID uuid.UUID,
	marketID string,
	isOpening bool, // true if opening/increasing position
	feeAmount int64,
	marginReserveAmount int64,
	marginReleaseAmount int64,
	realizedPnL int64,
	quoteAssetID AssetID,
	timestamp int64,
) (*Batch, error) {
	// PRE-CHECK: If opening, validate sufficient available for fee + margin
	if isOpening {
		required := feeAmount + marginReserveAmount
		if err := jg.balanceTracker.ValidateSufficientAvailable(userID, quoteAssetID, required); err != nil {
			return nil, fmt.Errorf("trade pre-check failed: %w", err)
		}
	}

	// PRE-CHECK: If releasing, validate sufficient reserved
	if marginReleaseAmount > 0 {
		if err := jg.balanceTracker.ValidateSufficientReserved(userID, quoteAssetID, marginReleaseAmount); err != nil {
			return nil, fmt.Errorf("margin release pre-check failed: %w", err)
		}
	}

	batchID := uuid.New()

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  fillID.String(),
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 4),
	}

	// Journal 1: Trading fee
	if feeAmount > 0 {
		feeJournal := Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      fillID.String(),
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFees, quoteAssetID),
			CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        feeAmount,
			JournalType:   JournalTypeTradeFee,
			Timestamp:     timestamp,
		}
		batch.Journals = append(batch.Journals, feeJournal)
	}

	// Journal 2: Margin reserve (if opening/increasing position)
	if marginReserveAmount > 0 {
		reserveJournal := Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      fillID.String(),
			Sequence:      jg.sequence,
			DebitAccount:  NewUserAccountKey(userID, SubTypeReserved, quoteAssetID),
			CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        marginReserveAmount,
			JournalType:   JournalTypeMarginReserve,
			Timestamp:     timestamp,
		}
		batch.Journals = append(batch.Journals, reserveJournal)
	}

	// Journal 3: Realized PnL (if closing/reducing position)
	if realizedPnL != 0 {
		var pnlJournal Journal
		if realizedPnL > 0 {
			// Profit: debit user:collateral, credit user:pnl
			pnlJournal = Journal{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				EventRef:      fillID.String(),
				Sequence:      jg.sequence,
				DebitAccount:  NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
				CreditAccount: NewUserAccountKey(userID, SubTypePnL, quoteAssetID),
				AssetID:       quoteAssetID,
				Amount:        realizedPnL,
				JournalType:   JournalTypeTradePnL,
				Timestamp:     timestamp,
			}
		} else {
			// Loss: debit user:pnl, credit user:collateral
			pnlJournal = Journal{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				EventRef:      fillID.String(),
				Sequence:      jg.sequence,
				DebitAccount:  NewUserAccountKey(userID, SubTypePnL, quoteAssetID),
				CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
				AssetID:       quoteAssetID,
				Amount:        -realizedPnL, // Convert to positive
				JournalType:   JournalTypeTradePnL,
				Timestamp:     timestamp,
			}
		}
		batch.Journals = append(batch.Journals, pnlJournal)
	}

	// Journal 4: Margin release (if closing/reducing position)
	if marginReleaseAmount > 0 {
		releaseJournal := Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      fillID.String(),
			Sequence:      jg.sequence,
			DebitAccount:  NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			CreditAccount: NewUserAccountKey(userID, SubTypeReserved, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        marginReleaseAmount,
			JournalType:   JournalTypeMarginRelease,
			Timestamp:     timestamp,
		}
		batch.Journals = append(batch.Journals, releaseJournal)
	}

	jg.sequence++
	return batch, nil
}

// GenerateFundingSettlement creates journals for all positions in funding epoch
func (jg *JournalGenerator) GenerateFundingSettlement(
	settlement *fpmath.FundingSettlement,
	quoteAssetID AssetID,
	timestamp int64,
) ([]*Batch, error) {
	batches := make([]*Batch, 0, len(settlement.Payments)+1)

	// Generate one batch per user
	for _, payment := range settlement.Payments {
		userID := uuid.UUID(payment.UserID)

		// Check available balance if user pays
		if payment.Payment > 0 {
			available := jg.balanceTracker.GetUserAvailableBalance(userID, quoteAssetID)

			if available < payment.Payment {
				// Insufficient balance - generate split journals
				batch, err := jg.generateFundingWithDeficit(
					userID,
					settlement.MarketID,
					settlement.EpochID,
					payment.Payment,
					available,
					quoteAssetID,
					timestamp,
				)
				if err != nil {
					return nil, err
				}
				batches = append(batches, batch)
				continue
			}
		}

		// Normal case: sufficient balance or user receives
		batch := jg.generateSingleFundingPayment(
			userID,
			settlement.MarketID,
			settlement.EpochID,
			payment.Payment,
			quoteAssetID,
			timestamp,
		)
		batches = append(batches, batch)
	}

	// Generate rounding fee batch if needed
	if settlement.RoundingFee != 0 {
		batch := jg.generateRoundingFeeBatch(
			settlement.MarketID,
			settlement.EpochID,
			settlement.RoundingFee,
			quoteAssetID,
			timestamp,
		)
		batches = append(batches, batch)
	}

	return batches, nil
}

// generateSingleFundingPayment creates journal for one user's funding payment
func (jg *JournalGenerator) generateSingleFundingPayment(
	userID uuid.UUID,
	marketID string,
	epochID int64,
	payment int64,
	quoteAssetID AssetID,
	timestamp int64,
) *Batch {
	batchID := uuid.New()
	eventRef := fmt.Sprintf("%s:%d:%s", marketID, epochID, userID.String())

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  eventRef,
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	var journal Journal

	if payment > 0 {
		// User pays funding
		journal = Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        payment,
			JournalType:   JournalTypeFundingSettle,
			Timestamp:     timestamp,
		}
	} else {
		// User receives funding
		journal = Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			CreditAccount: NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        -payment, // Convert to positive
			JournalType:   JournalTypeFundingSettle,
			Timestamp:     timestamp,
		}
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch
}

// generateFundingWithDeficit handles insufficient balance case
func (jg *JournalGenerator) generateFundingWithDeficit(
	userID uuid.UUID,
	marketID string,
	epochID int64,
	requiredPayment int64,
	availableBalance int64,
	quoteAssetID AssetID,
	timestamp int64,
) (*Batch, error) {
	batchID := uuid.New()
	eventRef := fmt.Sprintf("%s:%d:%s", marketID, epochID, userID.String())

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  eventRef,
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 2),
	}

	// Journal 1: Partial payment (what user can pay)
	if availableBalance > 0 {
		partialJournal := Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			CreditAccount: NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        availableBalance,
			JournalType:   JournalTypeFundingSettle,
			Timestamp:     timestamp,
		}
		batch.Journals = append(batch.Journals, partialJournal)
	}

	// Journal 2: Insurance fund covers deficit
	deficit := requiredPayment - availableBalance
	if deficit > 0 {
		insuranceJournal := Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			CreditAccount: NewSystemAccountKey("insurance", SubTypeSystemInsuranceFund, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        deficit,
			JournalType:   JournalTypeInsuranceFundDebit,
			Timestamp:     timestamp,
		}
		batch.Journals = append(batch.Journals, insuranceJournal)
	}

	jg.sequence++
	return batch, nil
}

// GenerateInsuranceCoverage creates journals for insurance fund covering a liquidation deficit.
// Per doc §9: when a liquidation results in bankruptcy (deficit > 0), the insurance fund
// covers the shortfall by transferring from system:insurance_fund to user:collateral.
func (jg *JournalGenerator) GenerateInsuranceCoverage(
	userID uuid.UUID,
	liquidationID uuid.UUID,
	coverageAmount int64,
	quoteAssetID AssetID,
	timestamp int64,
) (*Batch, error) {
	batchID := uuid.New()
	eventRef := fmt.Sprintf("%s:insurance", liquidationID)

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  eventRef,
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	journal := Journal{
		JournalID:     uuid.New(),
		BatchID:       batchID,
		EventRef:      eventRef,
		Sequence:      jg.sequence,
		DebitAccount:  NewUserAccountKey(userID, SubTypeCollateral, quoteAssetID),
		CreditAccount: NewSystemAccountKey("insurance", SubTypeSystemInsuranceFund, quoteAssetID),
		AssetID:       quoteAssetID,
		Amount:        coverageAmount,
		JournalType:   JournalTypeInsuranceFundDebit,
		Timestamp:     timestamp,
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch, nil
}

// generateRoundingFeeBatch posts rounding residual to fees account
func (jg *JournalGenerator) generateRoundingFeeBatch(
	marketID string,
	epochID int64,
	roundingFee int64,
	quoteAssetID AssetID,
	timestamp int64,
) *Batch {
	batchID := uuid.New()
	eventRef := fmt.Sprintf("%s:%d:rounding", marketID, epochID)

	batch := &Batch{
		BatchID:   batchID,
		EventRef:  eventRef,
		Sequence:  jg.sequence,
		Timestamp: timestamp,
		Journals:  make([]Journal, 0, 1),
	}

	var journal Journal

	if roundingFee > 0 {
		// Longs overpaid - transfer to fees
		journal = Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFees, quoteAssetID),
			CreditAccount: NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        roundingFee,
			JournalType:   JournalTypeAdjustment,
			Timestamp:     timestamp,
		}
	} else {
		// Shorts overpaid - transfer from fees
		journal = Journal{
			JournalID:     uuid.New(),
			BatchID:       batchID,
			EventRef:      eventRef,
			Sequence:      jg.sequence,
			DebitAccount:  NewSystemAccountKey(marketID, SubTypeSystemFundingPool, quoteAssetID),
			CreditAccount: NewSystemAccountKey(marketID, SubTypeSystemFees, quoteAssetID),
			AssetID:       quoteAssetID,
			Amount:        -roundingFee,
			JournalType:   JournalTypeAdjustment,
			Timestamp:     timestamp,
		}
	}

	batch.Journals = append(batch.Journals, journal)
	jg.sequence++

	return batch
}
