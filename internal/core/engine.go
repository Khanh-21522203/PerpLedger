// internal/core/engine.go (UPDATE)
package core

import (
	"PerpLedger/internal/event"
	"PerpLedger/internal/ledger"
	fpmath "PerpLedger/internal/math"
	"PerpLedger/internal/state"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// DeterministicCore is the single-threaded event processor
type DeterministicCore struct {
	sequence          int64
	hasher            *StateHasher
	balanceTracker    *ledger.BalanceTracker
	journalGen        *ledger.JournalGenerator
	validator         *ledger.InvariantValidator
	positionManager   *state.PositionManager
	fundingManager    *state.FundingManager
	marginCalc        *state.MarginCalculator
	liquidationMgr    *state.LiquidationManager
	riskParamsMgr     *state.RiskParamsManager
	idempotency       *IdempotencyChecker
	sequenceValidator *SequenceValidator

	persistChan    chan<- CoreOutput
	projectionChan chan<- CoreOutput
}

type CoreOutput struct {
	Envelope   *event.EventEnvelope
	Batch      *ledger.Batch
	StateDelta []byte
}

func NewDeterministicCore(
	startSequence int64,
	persistChan, projectionChan chan<- CoreOutput,
	dbChecker DBIdempotencyChecker,
) *DeterministicCore {
	balanceTracker := ledger.NewBalanceTracker()
	validator := ledger.NewInvariantValidator(balanceTracker)
	journalGen := ledger.NewJournalGenerator(startSequence, balanceTracker)
	positionMgr := state.NewPositionManager()
	riskParamsMgr := state.NewRiskParamsManager()
	marginCalc := state.NewMarginCalculator(positionMgr, balanceTracker, riskParamsMgr)
	liquidationMgr := state.NewLiquidationManager(positionMgr)

	// Initialize with capacity of 1M entries (configurable)
	idempotencyChecker := NewIdempotencyChecker(1_000_000, dbChecker)
	sequenceValidator := NewSequenceValidator()

	return &DeterministicCore{
		sequence:          startSequence,
		hasher:            NewStateHasher(),
		balanceTracker:    balanceTracker,
		journalGen:        journalGen,
		validator:         validator,
		positionManager:   positionMgr,
		fundingManager:    state.NewFundingManager(),
		marginCalc:        marginCalc,
		liquidationMgr:    liquidationMgr,
		riskParamsMgr:     riskParamsMgr,
		idempotency:       idempotencyChecker,
		sequenceValidator: sequenceValidator,
		persistChan:       persistChan,
		projectionChan:    projectionChan,
	}
}

// ProcessEvent is the main processing pipeline
func (c *DeterministicCore) ProcessEvent(evt event.Event) error {
	eventType := evt.EventType().String()
	idempotencyKey := evt.IdempotencyKey()

	// Step 1: Idempotency check (two-tier)
	isDuplicate := c.idempotency.IsDuplicate(eventType, idempotencyKey)

	// Step 2: Sequence validation
	partition := c.getPartition(evt)
	sourceSequence := evt.SourceSequence()

	// Special handling for price updates (gaps tolerated)
	if priceEvt, ok := evt.(*event.MarkPriceUpdate); ok {
		if err := c.sequenceValidator.ValidatePriceSequence(priceEvt.MarketID, priceEvt.PriceSequence); err != nil {
			return err
		}
	} else {
		// Regular sequence validation
		if err := c.sequenceValidator.ValidateSequence(partition, sourceSequence, idempotencyKey, isDuplicate); err != nil {
			return fmt.Errorf("sequence validation failed: %w", err)
		}
	}

	// If duplicate, skip processing
	if isDuplicate {
		return nil
	}

	// Step 3: Event dispatch - get batches
	var batches []*ledger.Batch
	var err error

	if settleEvt, ok := evt.(*event.FundingEpochSettle); ok {
		batches, err = c.handleFundingEpochSettle(settleEvt)
		if err != nil {
			return fmt.Errorf("funding settlement failed: %w", err)
		}
	} else {
		batch, dispatchErr := c.dispatchEvent(evt)
		if dispatchErr != nil {
			return fmt.Errorf("dispatch failed: %w", dispatchErr)
		}
		batches = []*ledger.Batch{batch}
	}

	// Step 4-9: Process each batch
	outputs := make([]CoreOutput, 0, len(batches))

	for _, batch := range batches {
		// Validate batch balance
		if err := c.validator.ValidateBatchBalance(batch); err != nil {
			panic(fmt.Sprintf("FATAL: unbalanced batch: %v", err))
		}

		// Apply batch to balances
		if err := c.balanceTracker.ApplyBatch(batch); err != nil {
			return fmt.Errorf("apply batch failed: %w", err)
		}

		// Compute state digest
		stateDigest := c.computeStateDigest(batch)

		// Compute state hash
		stateHash := c.hasher.ComputeHash(c.sequence, stateDigest)

		// Create envelope
		envelope := &event.EventEnvelope{
			Sequence:       c.sequence,
			IdempotencyKey: idempotencyKey,
			EventType:      evt.EventType(),
			MarketID:       evt.MarketID(),
			Timestamp:      c.getEventTimestamp(evt),
			SourceSequence: sourceSequence,
			StateHash:      stateHash,
			PrevHash:       c.hasher.GetPrevHash(),
		}

		output := CoreOutput{
			Envelope:   envelope,
			Batch:      batch,
			StateDelta: stateDigest,
		}

		outputs = append(outputs, output)
		c.sequence++
	}

	// Step 10: Post-checks
	if err := c.postCheckInvariants(evt); err != nil {
		panic(fmt.Sprintf("FATAL: invariant violated: %v", err))
	}

	// Step 11: Emit outputs
	for _, output := range outputs {
		select {
		case c.persistChan <- output:
		default:
			return fmt.Errorf("persist channel full")
		}

		select {
		case c.projectionChan <- output:
		default:
			return fmt.Errorf("projection channel full")
		}
	}

	// Step 12: Mark as processed (add to LRU)
	c.idempotency.MarkProcessed(eventType, idempotencyKey)

	return nil
}

// getPartition determines partition key for sequence validation
func (c *DeterministicCore) getPartition(evt event.Event) string {
	if marketID := evt.MarketID(); marketID != nil {
		return fmt.Sprintf("market:%s", *marketID)
	}
	return "global"
}

// getEventTimestamp extracts versioned timestamp from event
func (c *DeterministicCore) getEventTimestamp(evt event.Event) time.Time {
	// Extract timestamp based on event type
	switch e := evt.(type) {
	case *event.TradeFill:
		return e.Timestamp
	case *event.DepositConfirmed:
		return e.Timestamp
	case *event.WithdrawalRequested:
		return e.Timestamp
	case *event.WithdrawalConfirmed:
		return e.Timestamp
	case *event.WithdrawalRejected:
		return e.Timestamp
	case *event.MarkPriceUpdate:
		return time.UnixMicro(e.PriceTimestamp)
	case *event.FundingRateSnapshot:
		return time.UnixMicro(e.EpochTs)
	case *event.FundingEpochSettle:
		// Get timestamp from stored snapshot
		if snapshot, ok := c.fundingManager.GetFundingSnapshot(e.MarketID, e.EpochID); ok {
			return time.UnixMicro(snapshot.Timestamp)
		}
		return time.Now() // Fallback (should not happen)
	default:
		// Unknown event type - use current time (should not happen)
		return time.Now()
	}
}

// computeStateDigest creates canonical bytes for state hash
func (c *DeterministicCore) computeStateDigest(batch *ledger.Batch) []byte {
	// Collect all affected accounts
	affectedAccounts := make(map[ledger.AccountKey]bool)

	for _, j := range batch.Journals {
		affectedAccounts[j.DebitAccount] = true
		affectedAccounts[j.CreditAccount] = true
	}

	// Sort accounts deterministically
	accounts := make([]ledger.AccountKey, 0, len(affectedAccounts))
	for key := range affectedAccounts {
		accounts = append(accounts, key)
	}

	// Sort by AccountPath (deterministic string ordering)
	sort.Slice(accounts, func(i, j int) bool {
		return accounts[i].AccountPath() < accounts[j].AccountPath()
	})

	// Build digest
	digest := make([]byte, 0, len(accounts)*64)

	for _, key := range accounts {
		balance := c.balanceTracker.GetBalance(key)

		// Append account path
		path := key.AccountPath()
		digest = append(digest, byte(len(path)))
		digest = append(digest, []byte(path)...)

		// Append balance (8 bytes LE)
		digest = appendInt64LE(digest, balance)
	}

	return digest
}

func appendInt64LE(buf []byte, v int64) []byte {
	return append(buf,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}

// postCheckInvariants validates invariants after batch application
func (c *DeterministicCore) postCheckInvariants(evt event.Event) error {
	// Check user balance invariants for affected users
	switch e := evt.(type) {
	case *event.WithdrawalRequested:
		assetID, _ := ledger.GetAssetID(e.Asset)
		if err := c.balanceTracker.ValidateAvailableNonNegative(e.UserID, assetID); err != nil {
			return fmt.Errorf("post-check BS-01: %w", err)
		}
		if err := c.balanceTracker.ValidateReservedNonNegative(e.UserID, assetID); err != nil {
			return fmt.Errorf("post-check BS-02: %w", err)
		}

	case *event.TradeFill:
		// Get quote asset for market (hardcoded to USDT for MVP)
		assetID, _ := ledger.GetAssetID("USDT")
		if err := c.balanceTracker.ValidateAvailableNonNegative(e.UserID, assetID); err != nil {
			return fmt.Errorf("post-check BS-01: %w", err)
		}
		if err := c.balanceTracker.ValidateReservedNonNegative(e.UserID, assetID); err != nil {
			return fmt.Errorf("post-check BS-02: %w", err)
		}

	case *event.FundingEpochSettle:
		// Check funding pool is zero (INVARIANT L-07)
		quoteAssetID, _ := ledger.GetAssetID("USDT")
		if err := c.validator.ValidateFundingPoolZero(e.MarketID, quoteAssetID); err != nil {
			return fmt.Errorf("post-check L-07: %w", err)
		}
	}

	return nil
}

func (c *DeterministicCore) handleDepositConfirmed(evt *event.DepositConfirmed) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	return c.journalGen.GenerateDepositConfirmed(evt, assetID)
}

func (c *DeterministicCore) computeStateDigest(batch *ledger.Batch) []byte {
	digest := make([]byte, 0, 1024)

	for _, j := range batch.Journals {
		digest = append(digest, j.DebitAccount.AccountPath()...)
		digest = append(digest, j.CreditAccount.AccountPath()...)
	}

	return digest
}

// handleTradeFill with pre-trade margin check
func (c *DeterministicCore) handleTradeFill(evt *event.TradeFill) (*ledger.Batch, error) {
	quoteAssetID, _ := ledger.GetAssetID("USDT")

	pos := c.positionManager.GetPosition(evt.UserID, evt.Market)

	// Determine if opening/increasing
	var isOpening bool
	if pos == nil || pos.IsFlat() {
		isOpening = true
	} else if pos.Side == evt.TradeSide {
		isOpening = true
	} else {
		isOpening = false
	}

	// PRE-TRADE MARGIN CHECK (defensive)
	if isOpening {
		// Calculate required IM for new exposure
		params, ok := c.riskParamsMgr.GetRiskParams(evt.Market)
		if !ok {
			return nil, fmt.Errorf("no risk params for market %s", evt.Market)
		}

		currentSize := int64(0)
		if pos != nil {
			currentSize = pos.Size
		}

		newSize := currentSize + evt.Quantity
		newNotional := fpmath.ComputeNotional(
			newSize,
			evt.Price,
			fpmath.PriceConfig.Scale,
			fpmath.QuantityConfig.Scale,
			fpmath.QuoteConfig.Scale,
		)

		requiredIM := newNotional * params.IMFraction / 1_000_000
		effectiveCollateral := c.marginCalc.ComputeEffectiveCollateral(evt.UserID, quoteAssetID)

		if effectiveCollateral < requiredIM {
			// REJECT FILL - insufficient margin
			return nil, fmt.Errorf("fill rejected: insufficient margin (have=%d, need=%d)",
				effectiveCollateral, requiredIM)
		}
	}

	// Apply trade to position
	realizedPnL, _, _ := c.positionManager.ApplyTradeFill(
		evt.UserID,
		evt.Market,
		evt.TradeSide,
		evt.Quantity,
		evt.Price,
	)

	// Calculate margin amounts
	var marginReserve, marginRelease int64

	if isOpening {
		notional := fpmath.ComputeNotional(
			evt.Quantity,
			evt.Price,
			fpmath.PriceConfig.Scale,
			fpmath.QuantityConfig.Scale,
			fpmath.QuoteConfig.Scale,
		)

		params, _ := c.riskParamsMgr.GetRiskParams(evt.Market)
		marginReserve = notional * params.IMFraction / 1_000_000

	} else {
		// Proportional release
		if pos != nil && !pos.IsFlat() {
			reserved := c.balanceTracker.GetUserReservedBalance(evt.UserID, quoteAssetID)
			originalSize := pos.Size + evt.Quantity
			marginRelease = reserved * evt.Quantity / originalSize
		}
	}

	return c.journalGen.GenerateTradeFill(
		evt.UserID,
		evt.FillID,
		evt.Market,
		isOpening,
		evt.Fee,
		marginReserve,
		marginRelease,
		realizedPnL,
		quoteAssetID,
		evt.Timestamp.UnixMicro(),
	)
}

// handleMarkPriceUpdate processes mark price update
// handleMarkPriceUpdate with margin check
func (c *DeterministicCore) handleMarkPriceUpdate(evt *event.MarkPriceUpdate) (*ledger.Batch, error) {
	// Update mark price
	err := c.positionManager.UpdateMarkPrice(
		evt.MarketID,
		evt.MarkPrice,
		evt.PriceSequence,
		evt.PriceTimestamp,
	)

	if err != nil {
		return nil, err
	}

	// Trigger margin checks for all positions in this market
	c.checkMarginForMarket(evt.MarketID)

	// Return empty batch (mark price doesn't generate journals)
	return &ledger.Batch{
		BatchID:   uuid.New(),
		EventRef:  evt.IdempotencyKey(),
		Sequence:  c.sequence,
		Timestamp: evt.PriceTimestamp,
		Journals:  []ledger.Journal{},
	}, nil
}

// checkMarginForMarket checks all positions in a market
func (c *DeterministicCore) checkMarginForMarket(marketID string) {
	allPositions := c.positionManager.GetAllPositions()

	quoteAssetID, _ := ledger.GetAssetID("USDT")

	for _, pos := range allPositions {
		if pos.MarketID != marketID || pos.IsFlat() {
			continue
		}

		// Check margin health
		marginStatus := c.marginCalc.CheckMarginHealth(pos.UserID, quoteAssetID)

		switch marginStatus {
		case state.MarginStatusLiquidatable:
			// Trigger liquidation if not already triggered
			if pos.LiquidationState == state.LiquidationStateHealthy {
				c.triggerLiquidation(pos.UserID, pos.MarketID)
			}

		case state.MarginStatusAtRisk:
			// Transition to AtRisk if currently Healthy
			if pos.LiquidationState == state.LiquidationStateHealthy {
				pos.LiquidationState = state.LiquidationStateAtRisk
				pos.Version++
			}

		case state.MarginStatusHealthy:
			// Check if we can recover from AtRisk
			if pos.LiquidationState == state.LiquidationStateAtRisk {
				// Must recover to IM, not just MM
				marginFraction := c.marginCalc.ComputeMarginFraction(pos.UserID, quoteAssetID)
				params, _ := c.riskParamsMgr.GetRiskParams(pos.MarketID)

				if marginFraction >= params.IMFraction {
					pos.LiquidationState = state.LiquidationStateHealthy
					pos.Version++
				}
			}
		}
	}
}

// triggerLiquidation creates liquidation event
func (c *DeterministicCore) triggerLiquidation(userID uuid.UUID, marketID string) {
	liquidationID, err := c.liquidationMgr.TriggerLiquidation(userID, marketID, c.sequence)
	if err != nil {
		// Already in liquidation or no position
		return
	}

	// TODO: Emit LiquidationTriggered event to output channel
	// This will be published to NATS for liquidation engine to consume
	_ = liquidationID
}

// handleLiquidationFill processes liquidation fill
func (c *DeterministicCore) handleLiquidationFill(evt *event.LiquidationFill) (*ledger.Batch, error) {
	quoteAssetID, _ := ledger.GetAssetID("USDT")

	// Verify liquidation exists
	_, ok := c.liquidationMgr.GetActiveLiquidation(evt.LiquidationID)
	if !ok {
		return nil, fmt.Errorf("unknown liquidation_id: %s", evt.LiquidationID)
	}

	// Process fill in position manager
	realizedPnL, _, _ := c.positionManager.ApplyTradeFill(
		evt.UserID,
		evt.MarketID,
		evt.Side,
		evt.Quantity,
		evt.Price,
	)

	// Update liquidation state
	if err := c.liquidationMgr.ProcessLiquidationFill(evt.LiquidationID, evt.Quantity); err != nil {
		return nil, fmt.Errorf("liquidation fill processing failed: %w", err)
	}

	// Calculate proportional margin release
	reserved := c.balanceTracker.GetUserReservedBalance(evt.UserID, quoteAssetID)
	pos := c.positionManager.GetPosition(evt.UserID, evt.MarketID)

	var marginRelease int64
	if pos != nil && !pos.IsFlat() {
		originalSize := pos.Size + evt.Quantity
		marginRelease = reserved * evt.Quantity / originalSize
	} else {
		// Fully liquidated - release all
		marginRelease = reserved
	}

	// Generate journals
	batch, err := c.journalGen.GenerateTradeFill(
		evt.UserID,
		evt.FillID,
		evt.MarketID,
		false, // Liquidation is always closing
		evt.Fee,
		0, // No reserve
		marginRelease,
		realizedPnL,
		quoteAssetID,
		evt.Timestamp,
	)

	if err != nil {
		return nil, err
	}

	// POST-FILL: Check if margin recovered
	marginStatus := c.marginCalc.CheckMarginHealth(evt.UserID, quoteAssetID)
	if marginStatus == state.MarginStatusHealthy {
		c.liquidationMgr.CheckMarginRecovery(evt.LiquidationID, true)
	}

	return batch, nil
}

// handleFundingRateSnapshot stores the funding snapshot
func (c *DeterministicCore) handleFundingRateSnapshot(evt *event.FundingRateSnapshot) (*ledger.Batch, error) {
	// Validate and store snapshot
	err := c.fundingManager.StoreFundingSnapshot(
		evt.MarketID,
		evt.EpochID,
		evt.FundingRate,
		evt.MarkPrice,
		evt.EpochTs,
	)

	if err != nil {
		return nil, fmt.Errorf("funding snapshot validation failed: %w", err)
	}

	// No journal entries for snapshot (just state storage)
	return &ledger.Batch{
		BatchID:   uuid.New(),
		EventRef:  evt.IdempotencyKey(),
		Sequence:  c.sequence,
		Timestamp: evt.EpochTs,
		Journals:  []ledger.Journal{},
	}, nil
}

// handleFundingEpochSettle executes funding settlement
func (c *DeterministicCore) handleFundingEpochSettle(evt *event.FundingEpochSettle) ([]*ledger.Batch, error) {
	// Retrieve stored snapshot
	snapshot, ok := c.fundingManager.GetFundingSnapshot(evt.MarketID, evt.EpochID)
	if !ok {
		return nil, fmt.Errorf("no funding snapshot for %s epoch %d", evt.MarketID, evt.EpochID)
	}

	// Collect all positions in this market
	positions := c.collectPositionsForFunding(evt.MarketID)

	if len(positions) == 0 {
		// No positions - no settlement needed
		return []*ledger.Batch{}, nil
	}

	// Compute funding settlement
	settlement := fpmath.ComputeFundingSettlement(
		evt.MarketID,
		evt.EpochID,
		snapshot.FundingRate,
		snapshot.MarkPrice,
		positions,
	)

	// Generate journal batches
	quoteAssetID, _ := ledger.GetAssetID("USDT")
	batches, err := c.journalGen.GenerateFundingSettlement(
		settlement,
		quoteAssetID,
		snapshot.Timestamp,
	)

	if err != nil {
		return nil, fmt.Errorf("funding settlement generation failed: %w", err)
	}

	// Apply all batches
	for _, batch := range batches {
		// Validate batch balance
		if err := c.validator.ValidateBatchBalance(batch); err != nil {
			panic(fmt.Sprintf("FATAL: unbalanced funding batch: %v", err))
		}

		// Apply to balances
		if err := c.balanceTracker.ApplyBatch(batch); err != nil {
			return nil, fmt.Errorf("apply funding batch failed: %w", err)
		}
	}

	// POST-CHECK: Verify funding pool is zero (INVARIANT L-07)
	fundingPoolKey := ledger.NewSystemAccountKey(evt.MarketID, ledger.SubTypeSystemFundingPool, quoteAssetID)
	fundingPoolBalance := c.balanceTracker.GetBalance(fundingPoolKey)

	if fundingPoolBalance != 0 {
		panic(fmt.Sprintf("FATAL: funding pool not zero after settlement: %d", fundingPoolBalance))
	}

	return batches, nil
}

// collectPositionsForFunding gathers all positions in a market
func (c *DeterministicCore) collectPositionsForFunding(marketID string) []fpmath.PositionForFunding {
	allPositions := c.positionManager.GetAllPositions()

	result := make([]fpmath.PositionForFunding, 0)
	for _, pos := range allPositions {
		if pos.MarketID == marketID && !pos.IsFlat() {
			result = append(result, fpmath.PositionForFunding{
				UserID:   pos.UserID,
				Size:     pos.Size,
				SideSign: pos.SideSign(),
			})
		}
	}

	return result
}

func (c *DeterministicCore) handleWithdrawalRequested(evt *event.WithdrawalRequested) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	return c.journalGen.GenerateWithdrawalRequested(
		evt.UserID,
		evt.WithdrawalID,
		evt.Amount,
		assetID,
		evt.Timestamp.UnixMicro(),
	)
}

func (c *DeterministicCore) handleWithdrawalConfirmed(evt *event.WithdrawalConfirmed) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	return c.journalGen.GenerateWithdrawalConfirmed(
		evt.UserID,
		evt.WithdrawalID,
		evt.Amount,
		assetID,
		evt.Timestamp.UnixMicro(),
	)
}

func (c *DeterministicCore) handleWithdrawalRejected(evt *event.WithdrawalRejected) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	return c.journalGen.GenerateWithdrawalRejected(
		evt.UserID,
		evt.WithdrawalID,
		evt.Amount,
		assetID,
		evt.Timestamp.UnixMicro(),
	)
}

// Update dispatchEvent
func (c *DeterministicCore) dispatchEvent(evt event.Event) (*ledger.Batch, error) {
	switch e := evt.(type) {
	case *event.DepositConfirmed:
		return c.handleDepositConfirmed(e)
	case *event.WithdrawalRequested:
		return c.handleWithdrawalRequested(e)
	case *event.WithdrawalConfirmed:
		return c.handleWithdrawalConfirmed(e)
	case *event.WithdrawalRejected:
		return c.handleWithdrawalRejected(e)
	case *event.TradeFill:
		return c.handleTradeFill(e)
	case *event.MarkPriceUpdate:
		return c.handleMarkPriceUpdate(e)
	case *event.FundingRateSnapshot:
		return c.handleFundingRateSnapshot(e)
	case *event.LiquidationFill:
		return c.handleLiquidationFill(e)
	default:
		return nil, fmt.Errorf("unknown event type: %T", evt)
	}
}
