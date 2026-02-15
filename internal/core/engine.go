package core

import (
	"PerpLedger/internal/event"
	"PerpLedger/internal/ledger"
	fpmath "PerpLedger/internal/math"
	"PerpLedger/internal/observability"
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
	metrics           *observability.Metrics

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
	metrics *observability.Metrics,
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
		metrics:           metrics,
		persistChan:       persistChan,
		projectionChan:    projectionChan,
	}
}

// ProcessEvent is the main processing pipeline
func (c *DeterministicCore) ProcessEvent(evt event.Event) error {
	start := time.Now()
	eventType := evt.EventType().String()
	idempotencyKey := evt.IdempotencyKey()

	// Step 1: Idempotency check (two-tier)
	isDuplicate := c.idempotency.IsDuplicate(eventType, idempotencyKey)

	// Step 2: Sequence validation
	partition := c.getPartition(evt)
	sourceSequence := evt.SourceSequence()

	// Special handling for price updates (gaps tolerated)
	if priceEvt, ok := evt.(*event.MarkPriceUpdate); ok {
		if err := c.sequenceValidator.ValidatePriceSequence(priceEvt.Market, priceEvt.PriceSequence); err != nil {
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
		if c.metrics != nil {
			c.metrics.CoreEventsRejected.WithLabelValues(eventType, "duplicate").Inc()
		}
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
		// Skip validation and application for empty batches (state-only events
		// like MarkPriceUpdate, FundingRateSnapshot, RiskParamUpdate produce
		// no journals but still need an envelope in the event log).
		if len(batch.Journals) > 0 {
			// Validate batch balance
			if err := c.validator.ValidateBatchBalance(batch); err != nil {
				panic(fmt.Sprintf("FATAL: unbalanced batch: %v", err))
			}

			// Apply batch to balances
			if err := c.balanceTracker.ApplyBatch(batch); err != nil {
				return fmt.Errorf("apply batch failed: %w", err)
			}
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

	// Step 10: Post-batch margin checks for FundingEpochSettle
	// Per flow funding-epoch-settlement-flowchart: margin checks must run AFTER
	// funding payments are applied to balances (not before).
	if settleEvt, ok := evt.(*event.FundingEpochSettle); ok {
		snapshot, _ := c.fundingManager.GetFundingSnapshot(settleEvt.Market, settleEvt.EpochID)
		if snapshot != nil {
			allPositions := c.positionManager.GetAllPositions()
			for _, pos := range allPositions {
				if pos.MarketID == settleEvt.Market && !pos.IsFlat() {
					c.checkMarginForUser(pos.UserID, pos.MarketID, snapshot.Timestamp)
				}
			}
		}
	}

	// Step 10b: Post-checks
	if err := c.postCheckInvariants(evt); err != nil {
		panic(fmt.Sprintf("FATAL: invariant violated: %v", err))
	}

	// Step 11: Emit outputs
	// Per doc §12 (Concurrency): persist channel uses BLOCKING send (backpressure),
	// projection channel uses NON-BLOCKING send with silent drop.
	for _, output := range outputs {
		// Persistence: blocking send — the core stalls until the persistence
		// worker drains. This guarantees no event is lost.
		c.persistChan <- output

		// Projections: non-blocking send — drop on full. Projection workers
		// can rebuild from the event log if they fall behind.
		select {
		case c.projectionChan <- output:
		default:
			// Silently dropped — projection will catch up via rebuild
		}
	}

	// Step 12: Mark as processed (add to LRU)
	c.idempotency.MarkProcessed(eventType, idempotencyKey)

	// Record metrics
	if c.metrics != nil {
		c.metrics.CoreEventsApplied.WithLabelValues(eventType).Inc()
		c.metrics.CoreEventDuration.WithLabelValues(eventType).Observe(time.Since(start).Seconds())
		c.metrics.CoreSequence.Set(float64(c.sequence))
	}

	return nil
}

// getPartition determines partition key for sequence validation
func (c *DeterministicCore) getPartition(evt event.Event) string {
	if marketID := evt.MarketID(); marketID != nil {
		return fmt.Sprintf("market:%s", *marketID)
	}
	return "global"
}

// getEventTimestamp extracts versioned timestamp from event.
// Per doc §3.3: the core MUST NOT call time.Now(). All timestamps are versioned inputs.
func (c *DeterministicCore) getEventTimestamp(evt event.Event) time.Time {
	switch e := evt.(type) {
	case *event.DepositInitiated:
		return e.Timestamp
	case *event.DepositConfirmed:
		return e.Timestamp
	case *event.WithdrawalRequested:
		return e.Timestamp
	case *event.WithdrawalConfirmed:
		return e.Timestamp
	case *event.WithdrawalRejected:
		return e.Timestamp
	case *event.TradeFill:
		return e.Timestamp
	case *event.MarkPriceUpdate:
		return time.UnixMicro(e.PriceTimestamp)
	case *event.FundingRateSnapshot:
		return time.UnixMicro(e.EpochTs)
	case *event.FundingEpochSettle:
		snapshot, ok := c.fundingManager.GetFundingSnapshot(e.Market, e.EpochID)
		if !ok {
			panic(fmt.Sprintf("FATAL: no funding snapshot for %s epoch %d — cannot derive deterministic timestamp", e.Market, e.EpochID))
		}
		return time.UnixMicro(snapshot.Timestamp)
	case *event.LiquidationFill:
		return time.UnixMicro(e.Timestamp)
	case *event.LiquidationCompleted:
		return time.UnixMicro(e.Timestamp)
	case *event.RiskParamUpdate:
		return time.UnixMicro(e.Timestamp)
	default:
		panic(fmt.Sprintf("FATAL: getEventTimestamp called with unhandled event type %T — deterministic core cannot use wall-clock time", evt))
	}
}

// computeStateDigest creates canonical bytes for state hash
func (c *DeterministicCore) computeStateDigest(batch *ledger.Batch) []byte {
	// Collect all affected accounts
	affectedAccounts := make(map[ledger.AccountKey]bool)

	if batch != nil {
		for _, j := range batch.Journals {
			affectedAccounts[j.DebitAccount] = true
			affectedAccounts[j.CreditAccount] = true
		}
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
		if err := c.validator.ValidateFundingPoolZero(e.Market, quoteAssetID); err != nil {
			return fmt.Errorf("post-check L-07: %w", err)
		}
	}

	// Periodic global balance check (INVARIANT L-06)
	// Per doc ledger-invariants-verification: verify sum of all accounts == 0
	if c.sequence > 0 && c.sequence%1000 == 0 {
		totals := c.balanceTracker.ComputeGlobalBalance()
		for assetID, total := range totals {
			if total != 0 {
				return fmt.Errorf("post-check L-06: global balance non-zero for asset %d: %d (at seq %d)",
					assetID, total, c.sequence)
			}
		}
	}

	return nil
}

func (c *DeterministicCore) handleDepositInitiated(evt *event.DepositInitiated) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	return c.journalGen.GenerateDepositInitiated(evt, assetID)
}

func (c *DeterministicCore) handleDepositConfirmed(evt *event.DepositConfirmed) (*ledger.Batch, error) {
	assetID, ok := ledger.GetAssetID(evt.Asset)
	if !ok {
		return nil, fmt.Errorf("unknown asset: %s", evt.Asset)
	}

	batch, err := c.journalGen.GenerateDepositConfirmed(evt, assetID)
	if err != nil {
		return nil, err
	}

	// POST-DEPOSIT MARGIN CHECK
	// Per flow event-dispatch-by-type: DepositConfirmed triggers margin check.
	// Collateral increased — may resolve AtRisk status for user's positions.
	positions := c.positionManager.GetUserPositions(evt.UserID)
	for _, pos := range positions {
		if !pos.IsFlat() {
			c.checkMarginForUser(evt.UserID, pos.MarketID, evt.Timestamp.UnixMicro())
		}
	}

	return batch, nil
}

// handleTradeFill with pre-trade margin check.
// Per flow position-lifecycle-flowchart: handles open, increase, partial reduce,
// full close, and flip (decomposed into close + open).
func (c *DeterministicCore) handleTradeFill(evt *event.TradeFill) (*ledger.Batch, error) {
	quoteAssetID, _ := ledger.GetAssetID("USDT")

	pos := c.positionManager.GetPosition(evt.UserID, evt.Market)

	// Classify the fill action
	// Per flow position-lifecycle-flowchart: 5 cases based on position state and fill direction
	type fillAction int
	const (
		fillActionOpen     fillAction = iota // No position or flat → new position
		fillActionIncrease                   // Same side → increase existing
		fillActionReduce                     // Opposite side, qty < pos.Size → partial reduce
		fillActionClose                      // Opposite side, qty == pos.Size → full close
		fillActionFlip                       // Opposite side, qty > pos.Size → close + open opposite
	)

	var action fillAction
	if pos == nil || pos.IsFlat() {
		action = fillActionOpen
	} else if pos.Side == evt.TradeSide {
		action = fillActionIncrease
	} else if evt.Quantity < pos.Size {
		action = fillActionReduce
	} else if evt.Quantity == pos.Size {
		action = fillActionClose
	} else {
		action = fillActionFlip
	}

	isOpening := action == fillActionOpen || action == fillActionIncrease
	isFlip := action == fillActionFlip

	// REDUCE-ONLY MODE ENFORCEMENT
	// Per flow reduce-only-mode-enforcement-flowchart:
	// - AtRisk users can only reduce/close, NOT open/increase/flip
	// - Flip is rejected because it creates new exposure
	if pos != nil && pos.LiquidationState == state.LiquidationStateAtRisk {
		if isOpening || isFlip {
			return nil, fmt.Errorf("fill rejected: user is in reduce-only mode (AtRisk) for market %s", evt.Market)
		}
	}

	// PRE-TRADE MARGIN CHECK (defensive)
	// Per flow pre-trade-margin-check-flowchart: compute total IM across ALL positions
	if isOpening || isFlip {
		params, ok := c.riskParamsMgr.GetRiskParams(evt.Market)
		if !ok {
			return nil, fmt.Errorf("no risk params for market %s", evt.Market)
		}

		// Per flow: use mark_price for notional computation, not fill price
		markPrice, hasMarkPrice := c.positionManager.GetMarkPrice(evt.Market)
		if !hasMarkPrice {
			// Fallback to fill price if no mark price available yet
			markPrice = evt.Price
		}

		// Compute new size for this market's position
		var newSizeThisMarket int64
		if isFlip {
			// Flip: new position size = fill.qty - old_position.size
			newSizeThisMarket = evt.Quantity - pos.Size
		} else if pos != nil && !pos.IsFlat() {
			// Increase: current + fill
			newSizeThisMarket = pos.Size + evt.Quantity
		} else {
			// New position
			newSizeThisMarket = evt.Quantity
		}

		newNotionalThisMarket := fpmath.ComputeNotional(
			newSizeThisMarket,
			markPrice,
			fpmath.PriceConfig.Scale,
			fpmath.QuantityConfig.Scale,
			fpmath.QuoteConfig.Scale,
		)
		requiredIMThisMarket := newNotionalThisMarket * params.IMFraction / 1_000_000

		// Per flow: compute total IM across ALL positions (not just this market)
		totalRequiredIM := requiredIMThisMarket
		allPositions := c.positionManager.GetUserPositions(evt.UserID)
		for _, otherPos := range allPositions {
			if otherPos.MarketID == evt.Market || otherPos.IsFlat() {
				continue
			}
			otherParams, ok := c.riskParamsMgr.GetRiskParams(otherPos.MarketID)
			if !ok {
				continue
			}
			otherNotional, err := c.positionManager.ComputePositionNotional(otherPos)
			if err != nil {
				continue
			}
			totalRequiredIM += otherNotional * otherParams.IMFraction / 1_000_000
		}

		effectiveCollateral := c.marginCalc.ComputeEffectiveCollateral(evt.UserID, quoteAssetID)

		if effectiveCollateral < totalRequiredIM {
			return nil, fmt.Errorf("fill rejected: insufficient margin (have=%d, need=%d)",
				effectiveCollateral, totalRequiredIM)
		}
	}

	// Capture reserved balance BEFORE position mutation for margin release calculation
	reservedBeforeFill := c.balanceTracker.GetUserReservedBalance(evt.UserID, quoteAssetID)
	oldSize := int64(0)
	if pos != nil {
		oldSize = pos.Size
	}

	// Apply trade to position
	realizedPnL, _, _ := c.positionManager.ApplyTradeFill(
		evt.UserID,
		evt.Market,
		evt.TradeSide,
		evt.Quantity,
		evt.Price,
	)

	// Calculate margin amounts based on fill action
	var marginReserve, marginRelease int64

	switch action {
	case fillActionOpen, fillActionIncrease:
		// Reserve IM for new/additional exposure
		notional := fpmath.ComputeNotional(
			evt.Quantity,
			evt.Price,
			fpmath.PriceConfig.Scale,
			fpmath.QuantityConfig.Scale,
			fpmath.QuoteConfig.Scale,
		)
		params, _ := c.riskParamsMgr.GetRiskParams(evt.Market)
		marginReserve = notional * params.IMFraction / 1_000_000

	case fillActionReduce:
		// Per flow position-lifecycle-flowchart: proportional IM release
		if oldSize > 0 {
			marginRelease = reservedBeforeFill * evt.Quantity / oldSize
		}

	case fillActionClose:
		// Per flow position-lifecycle-flowchart: release ALL reserved margin
		marginRelease = reservedBeforeFill

	case fillActionFlip:
		// Per flow position-lifecycle-flowchart: close existing (release all) + open new (reserve)
		// Step 1: Release ALL margin from closed position
		marginRelease = reservedBeforeFill

		// Step 2: Reserve IM for new position in opposite direction
		remainingQty := evt.Quantity - oldSize
		notional := fpmath.ComputeNotional(
			remainingQty,
			evt.Price,
			fpmath.PriceConfig.Scale,
			fpmath.QuantityConfig.Scale,
			fpmath.QuoteConfig.Scale,
		)
		params, _ := c.riskParamsMgr.GetRiskParams(evt.Market)
		marginReserve = notional * params.IMFraction / 1_000_000
	}

	batch, err := c.journalGen.GenerateTradeFill(
		evt.UserID,
		evt.FillID,
		evt.Market,
		isOpening || isFlip,
		evt.Fee,
		marginReserve,
		marginRelease,
		realizedPnL,
		quoteAssetID,
		evt.Timestamp.UnixMicro(),
	)
	if err != nil {
		return nil, err
	}

	// POST-TRADE MARGIN CHECK
	// Per doc margin-check-triggers: TradeFill triggers margin check for the user.
	// Fee deduction or PnL realization may push user below MM.
	c.checkMarginForUser(evt.UserID, evt.Market, evt.Timestamp.UnixMicro())

	return batch, nil
}

// checkMarginForUser checks margin health for a single user in a specific market.
func (c *DeterministicCore) checkMarginForUser(userID uuid.UUID, marketID string, parentTimestamp int64) {
	quoteAssetID, _ := ledger.GetAssetID("USDT")

	pos := c.positionManager.GetPosition(userID, marketID)
	if pos == nil || pos.IsFlat() {
		return
	}

	marginStatus := c.marginCalc.CheckMarginHealth(userID, quoteAssetID)
	prevState := pos.LiquidationState

	switch marginStatus {
	case state.MarginStatusLiquidatable:
		// Per flow margin-check-triggers-flowchart + liquidation-state-machine-flowchart:
		// Healthy → AtRisk first (emit ReduceOnlyModeEntered), then trigger liquidation.
		// AtRisk → InLiquidation (trigger liquidation).
		switch pos.LiquidationState {
		case state.LiquidationStateHealthy:
			// Transition through AtRisk first so ReduceOnlyModeEntered is emitted
			pos.LiquidationState = state.LiquidationStateAtRisk
			pos.Version++
			c.emitReduceOnlyEvents(userID, marketID, prevState, pos.LiquidationState, parentTimestamp)
			// Now trigger liquidation (AtRisk → InLiquidation)
			c.triggerLiquidation(userID, marketID, parentTimestamp)
		case state.LiquidationStateAtRisk:
			// Already AtRisk, now below MM → trigger liquidation
			c.triggerLiquidation(userID, marketID, parentTimestamp)
		case state.LiquidationStatePartiallyLiquidated:
			// Still below MM after partial liquidation — stay in liquidation, no action needed
			// Liquidation engine will continue sending fills
		}

	case state.MarginStatusAtRisk:
		if pos.LiquidationState == state.LiquidationStateHealthy {
			pos.LiquidationState = state.LiquidationStateAtRisk
			pos.Version++
		}

	case state.MarginStatusHealthy:
		// Per flow reduce-only-mode-enforcement-flowchart:
		// Recovery requires margin_fraction >= im_fraction (hysteresis)
		if pos.LiquidationState == state.LiquidationStateAtRisk ||
			pos.LiquidationState == state.LiquidationStatePartiallyLiquidated {
			marginFraction := c.marginCalc.ComputeMarginFraction(userID, quoteAssetID)
			params, _ := c.riskParamsMgr.GetRiskParams(marketID)
			if marginFraction >= params.IMFraction {
				// Recover: stop liquidation if partially liquidated
				if pos.LiquidationState == state.LiquidationStatePartiallyLiquidated {
					if pos.LiquidationID != nil {
						liqID, err := uuid.Parse(*pos.LiquidationID)
						if err == nil {
							c.liquidationMgr.CheckMarginRecovery(liqID, true)
						}
					}
				}
				pos.LiquidationState = state.LiquidationStateHealthy
				pos.LiquidationID = nil
				pos.Version++
			}
		}
	}

	// Emit ReduceOnlyMode events on AtRisk transitions
	// (Skip if already emitted above in the Liquidatable→Healthy→AtRisk path)
	if !(prevState == state.LiquidationStateHealthy && marginStatus == state.MarginStatusLiquidatable) {
		c.emitReduceOnlyEvents(userID, marketID, prevState, pos.LiquidationState, parentTimestamp)
	}
}

// emitReduceOnlyEvents publishes ReduceOnlyModeEntered/Exited to the projection channel
// when a position transitions to/from AtRisk state.
// Per glossary: ReduceOnlyModeEntered and ReduceOnlyModeExited are outbound events.
func (c *DeterministicCore) emitReduceOnlyEvents(
	userID uuid.UUID,
	marketID string,
	prevState, newState state.LiquidationState,
	parentTimestamp int64,
) {
	if prevState == newState {
		return
	}

	var evtType event.EventType

	if newState == state.LiquidationStateAtRisk && prevState == state.LiquidationStateHealthy {
		evtType = event.EventTypeReduceOnlyModeEntered
	} else if newState == state.LiquidationStateHealthy && prevState == state.LiquidationStateAtRisk {
		evtType = event.EventTypeReduceOnlyModeExited
	} else {
		return
	}

	output := CoreOutput{
		Envelope: &event.EventEnvelope{
			Sequence:       c.sequence,
			IdempotencyKey: fmt.Sprintf("reduce_only:%s:%s:%d", userID, marketID, c.sequence),
			EventType:      evtType,
			MarketID:       &marketID,
			Timestamp:      time.UnixMicro(parentTimestamp),
		},
	}

	// Non-blocking send to projection channel only (informational event)
	select {
	case c.projectionChan <- output:
	default:
	}
}

// handleMarkPriceUpdate processes mark price update and triggers margin checks.
// Mark price updates do NOT generate journal entries — they only mutate in-memory
// state (mark price cache) and may trigger liquidation side-effects.
func (c *DeterministicCore) handleMarkPriceUpdate(evt *event.MarkPriceUpdate) (*ledger.Batch, error) {
	err := c.positionManager.UpdateMarkPrice(
		evt.Market,
		evt.MarkPrice,
		evt.PriceSequence,
		evt.PriceTimestamp,
	)

	if err != nil {
		return nil, err
	}

	// Trigger margin checks for all positions in this market
	c.checkMarginForMarket(evt.Market, evt.PriceTimestamp)

	// Return empty batch (mark price doesn't generate journals)
	return &ledger.Batch{
		BatchID:   uuid.New(),
		EventRef:  evt.IdempotencyKey(),
		Sequence:  c.sequence,
		Timestamp: evt.PriceTimestamp,
		Journals:  []ledger.Journal{},
	}, nil
}

// checkMarginForMarket checks all positions in a market.
// parentTimestamp is the deterministic timestamp from the triggering event.
func (c *DeterministicCore) checkMarginForMarket(marketID string, parentTimestamp int64) {
	allPositions := c.positionManager.GetAllPositions()

	quoteAssetID, _ := ledger.GetAssetID("USDT")

	for _, pos := range allPositions {
		if pos.MarketID != marketID || pos.IsFlat() {
			continue
		}

		// Check margin health
		marginStatus := c.marginCalc.CheckMarginHealth(pos.UserID, quoteAssetID)
		prevState := pos.LiquidationState

		switch marginStatus {
		case state.MarginStatusLiquidatable:
			// Per flow: Healthy → AtRisk (emit ReduceOnlyModeEntered) → InLiquidation
			// AtRisk → InLiquidation directly
			switch pos.LiquidationState {
			case state.LiquidationStateHealthy:
				pos.LiquidationState = state.LiquidationStateAtRisk
				pos.Version++
				c.emitReduceOnlyEvents(pos.UserID, pos.MarketID, prevState, pos.LiquidationState, parentTimestamp)
				c.triggerLiquidation(pos.UserID, pos.MarketID, parentTimestamp)
			case state.LiquidationStateAtRisk:
				c.triggerLiquidation(pos.UserID, pos.MarketID, parentTimestamp)
			case state.LiquidationStatePartiallyLiquidated:
				// Still below MM — liquidation engine continues sending fills
			}

		case state.MarginStatusAtRisk:
			if pos.LiquidationState == state.LiquidationStateHealthy {
				pos.LiquidationState = state.LiquidationStateAtRisk
				pos.Version++
			}

		case state.MarginStatusHealthy:
			// Per flow: recovery requires margin_fraction >= im_fraction (hysteresis)
			if pos.LiquidationState == state.LiquidationStateAtRisk ||
				pos.LiquidationState == state.LiquidationStatePartiallyLiquidated {
				marginFraction := c.marginCalc.ComputeMarginFraction(pos.UserID, quoteAssetID)
				params, _ := c.riskParamsMgr.GetRiskParams(pos.MarketID)

				if marginFraction >= params.IMFraction {
					if pos.LiquidationState == state.LiquidationStatePartiallyLiquidated {
						if pos.LiquidationID != nil {
							liqID, err := uuid.Parse(*pos.LiquidationID)
							if err == nil {
								c.liquidationMgr.CheckMarginRecovery(liqID, true)
							}
						}
					}
					pos.LiquidationState = state.LiquidationStateHealthy
					pos.LiquidationID = nil
					pos.Version++
				}
			}
		}

		// Emit ReduceOnlyMode events on AtRisk transitions
		// (Skip if already emitted above in the Liquidatable→Healthy→AtRisk path)
		if !(prevState == state.LiquidationStateHealthy && marginStatus == state.MarginStatusLiquidatable) {
			c.emitReduceOnlyEvents(pos.UserID, pos.MarketID, prevState, pos.LiquidationState, parentTimestamp)
		}
	}
}

// triggerLiquidation creates a LiquidationTriggered event and emits it.
// Per docs §6.2: published to NATS "liquidations.triggered" for the external
// liquidation engine to begin unwinding the position.
// parentTimestamp is the deterministic timestamp from the triggering event —
// the core MUST NOT call time.Now() (doc §3.3).
func (c *DeterministicCore) triggerLiquidation(userID uuid.UUID, marketID string, parentTimestamp int64) {
	// Allocate a dedicated sequence for this derived event to avoid collision
	liqSeq := c.sequence
	c.sequence++

	liquidationID, err := c.liquidationMgr.TriggerLiquidation(userID, marketID, liqSeq)
	if err != nil {
		// Already in liquidation or no position
		return
	}

	triggerEvt := &event.LiquidationTriggered{
		LiquidationID: liquidationID,
		UserID:        userID,
		Market:        marketID,
		Sequence:      liqSeq,
		Timestamp:     parentTimestamp, // Deterministic: derived from parent event
	}

	// Compute state hash for this derived event
	stateDigest := c.computeStateDigest(nil)
	stateHash := c.hasher.ComputeHash(liqSeq, stateDigest)

	// Emit to persist channel so it's recorded in the event log
	output := CoreOutput{
		Envelope: &event.EventEnvelope{
			Sequence:       liqSeq,
			IdempotencyKey: triggerEvt.IdempotencyKey(),
			EventType:      event.EventTypeLiquidationTriggered,
			MarketID:       triggerEvt.MarketID(),
			Timestamp:      time.UnixMicro(parentTimestamp),
			StateHash:      stateHash,
			PrevHash:       c.hasher.GetPrevHash(),
		},
	}

	// Blocking send — guarantees no event is lost (doc §12)
	c.persistChan <- output

	// Non-blocking projection send
	select {
	case c.projectionChan <- output:
	default:
	}
}

// handleLiquidationFill processes a liquidation fill from the liquidation engine.
// Liquidation fills always CLOSE positions (never open), so marginReserve = 0.
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
		evt.Market,
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
	pos := c.positionManager.GetPosition(evt.UserID, evt.Market)

	var marginRelease int64
	if pos != nil && !pos.IsFlat() {
		// TRICKY: pos.Size is AFTER the fill was applied, so originalSize = current + filled
		originalSize := pos.Size + evt.Quantity
		marginRelease = reserved * evt.Quantity / originalSize
	} else {
		// Fully liquidated - release all remaining reserved margin
		marginRelease = reserved
	}

	// Generate journals
	batch, err := c.journalGen.GenerateTradeFill(
		evt.UserID,
		evt.FillID,
		evt.Market,
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

	// POST-FILL: Check if margin recovered after partial liquidation
	marginStatus := c.marginCalc.CheckMarginHealth(evt.UserID, quoteAssetID)
	if marginStatus == state.MarginStatusHealthy {
		c.liquidationMgr.CheckMarginRecovery(evt.LiquidationID, true)
	}

	return batch, nil
}

// handleFundingRateSnapshot stores the funding snapshot for later settlement.
// No journal entries are generated — this only records state for the upcoming
// FundingEpochSettle event.
func (c *DeterministicCore) handleFundingRateSnapshot(evt *event.FundingRateSnapshot) (*ledger.Batch, error) {
	err := c.fundingManager.StoreFundingSnapshot(
		evt.Market,
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

// handleFundingEpochSettle executes funding settlement for all positions in a market.
// TRICKY: This returns multiple batches (one per user + optional rounding batch).
// The batches are NOT applied here — they are returned to ProcessEvent which
// applies them via the standard pipeline (validate → apply → hash → emit).
func (c *DeterministicCore) handleFundingEpochSettle(evt *event.FundingEpochSettle) ([]*ledger.Batch, error) {
	// Retrieve stored snapshot
	snapshot, ok := c.fundingManager.GetFundingSnapshot(evt.Market, evt.EpochID)
	if !ok {
		return nil, fmt.Errorf("no funding snapshot for %s epoch %d", evt.Market, evt.EpochID)
	}

	// Collect all positions in this market
	positions := c.collectPositionsForFunding(evt.Market)

	if len(positions) == 0 {
		return []*ledger.Batch{}, nil
	}

	// Compute funding settlement (deterministic: positions sorted by user_id)
	settlement := fpmath.ComputeFundingSettlement(
		evt.Market,
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

	// Update LastFundingEpoch on all settled positions.
	// Per doc §8: prevents double-settlement if the same epoch is replayed.
	allPositions := c.positionManager.GetAllPositions()
	for _, pos := range allPositions {
		if pos.MarketID == evt.Market && !pos.IsFlat() {
			pos.LastFundingEpoch = evt.EpochID
			pos.Version++
		}
	}

	// NOTE: POST-FUNDING MARGIN CHECK is deferred to ProcessEvent after batches
	// are applied to balances. Running it here would use stale (pre-funding) balances.
	// See postBatchMarginChecks() called from ProcessEvent.

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

	batch, err := c.journalGen.GenerateWithdrawalRequested(
		evt.UserID,
		evt.WithdrawalID,
		evt.Amount,
		assetID,
		evt.Timestamp.UnixMicro(),
	)
	if err != nil {
		return nil, err
	}

	// POST-WITHDRAWAL MARGIN CHECK
	// Per flow event-dispatch-by-type: WithdrawalRequested triggers margin check.
	// Collateral decreased — may push user below MM threshold.
	positions := c.positionManager.GetUserPositions(evt.UserID)
	for _, pos := range positions {
		if !pos.IsFlat() {
			c.checkMarginForUser(evt.UserID, pos.MarketID, evt.Timestamp.UnixMicro())
		}
	}

	return batch, nil
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

// handleLiquidationCompleted processes the completion of a liquidation.
// Per doc §9: if deficit > 0, bankruptcy occurred and the insurance fund covers it.
// If the insurance fund is insufficient, the system should escalate to ADL.
func (c *DeterministicCore) handleLiquidationCompleted(evt *event.LiquidationCompleted) (*ledger.Batch, error) {
	quoteAssetID, _ := ledger.GetAssetID("USDT")

	// Verify liquidation exists
	liq, ok := c.liquidationMgr.GetActiveLiquidation(evt.LiquidationID)
	if !ok {
		return nil, fmt.Errorf("unknown liquidation_id: %s", evt.LiquidationID)
	}

	pos := c.positionManager.GetPosition(liq.UserID, liq.MarketID)
	if pos != nil {
		// Transition to Closed
		if pos.LiquidationState.CanTransitionTo(state.LiquidationStateClosed) {
			pos.LiquidationState = state.LiquidationStateClosed
			pos.Version++
		}
	}

	if evt.Deficit <= 0 {
		// No deficit — clean completion, no journal entries needed
		return &ledger.Batch{
			BatchID:  uuid.New(),
			EventRef: evt.IdempotencyKey(),
			Sequence: c.sequence,
			Timestamp: evt.Timestamp,
			Journals: []ledger.Journal{},
		}, nil
	}

	// Deficit > 0: bankruptcy occurred.
	// Insurance fund covers the shortfall.
	insuranceFundKey := ledger.NewSystemAccountKey("insurance", ledger.SubTypeCollateral, quoteAssetID)
	insuranceBalance := c.balanceTracker.GetBalance(insuranceFundKey)

	covered := evt.Deficit
	if insuranceBalance < evt.Deficit {
		// Insurance fund insufficient — cover what we can.
		// TRICKY: remaining deficit should trigger ADL escalation (post-MVP).
		covered = insuranceBalance
	}

	if covered > 0 {
		// Generate insurance fund coverage journal
		return c.journalGen.GenerateInsuranceCoverage(
			liq.UserID,
			evt.LiquidationID,
			covered,
			quoteAssetID,
			evt.Timestamp,
		)
	}

	// No coverage possible — empty batch
	return &ledger.Batch{
		BatchID:  uuid.New(),
		EventRef: evt.IdempotencyKey(),
		Sequence: c.sequence,
		Timestamp: evt.Timestamp,
		Journals: []ledger.Journal{},
	}, nil
}

// handleRiskParamUpdate processes a risk parameter update for a market.
// Per docs §8: update in-memory risk params, recompute margin for all positions
// in the market, trigger liquidation for any user whose margin now violates MM.
// Per flow risk-param-update-flowchart: after updating params, adjust reserved
// margin for each position (reserve more if new_IM > old_reserved, release if less).
func (c *DeterministicCore) handleRiskParamUpdate(evt *event.RiskParamUpdate) (*ledger.Batch, error) {
	newParams := &state.RiskParams{
		MarketID:     evt.Market,
		IMFraction:   evt.IMFraction,
		MMFraction:   evt.MMFraction,
		MaxLeverage:  evt.MaxLeverage,
		TickSize:     evt.TickSize,
		LotSize:      evt.LotSize,
		EffectiveSeq: evt.EffectiveSeq,
	}

	if err := c.riskParamsMgr.UpdateRiskParams(newParams); err != nil {
		return nil, fmt.Errorf("risk param update rejected: %w", err)
	}

	quoteAssetID, _ := ledger.GetAssetID("USDT")
	batchID := uuid.New()
	batch := &ledger.Batch{
		BatchID:   batchID,
		EventRef:  evt.IdempotencyKey(),
		Sequence:  c.sequence,
		Timestamp: evt.Timestamp,
		Journals:  make([]ledger.Journal, 0),
	}

	// Adjust reserved margin for all positions in this market
	allPositions := c.positionManager.GetAllPositions()
	for _, pos := range allPositions {
		if pos.MarketID != evt.Market || pos.IsFlat() {
			continue
		}

		// Compute required IM under new params
		notional, err := c.positionManager.ComputePositionNotional(pos)
		if err != nil {
			continue
		}
		requiredIM := notional * newParams.IMFraction / 1_000_000

		// Current reserved balance for this user
		currentReserved := c.balanceTracker.GetUserReservedBalance(pos.UserID, quoteAssetID)

		if requiredIM > currentReserved {
			// Need to reserve more: collateral → reserved
			delta := requiredIM - currentReserved
			available := c.balanceTracker.GetUserAvailableBalance(pos.UserID, quoteAssetID)
			if delta > available {
				delta = available // Reserve what's available; margin check below handles the rest
			}
			if delta > 0 {
				batch.Journals = append(batch.Journals, ledger.Journal{
					JournalID:     uuid.New(),
					BatchID:       batchID,
					EventRef:      evt.IdempotencyKey(),
					Sequence:      c.sequence,
					DebitAccount:  ledger.NewUserAccountKey(pos.UserID, ledger.SubTypeReserved, quoteAssetID),
					CreditAccount: ledger.NewUserAccountKey(pos.UserID, ledger.SubTypeCollateral, quoteAssetID),
					AssetID:       quoteAssetID,
					Amount:        delta,
					JournalType:   ledger.JournalTypeMarginReserve,
					Timestamp:     evt.Timestamp,
				})
			}
		} else if requiredIM < currentReserved {
			// Release excess: reserved → collateral
			delta := currentReserved - requiredIM
			batch.Journals = append(batch.Journals, ledger.Journal{
				JournalID:     uuid.New(),
				BatchID:       batchID,
				EventRef:      evt.IdempotencyKey(),
				Sequence:      c.sequence,
				DebitAccount:  ledger.NewUserAccountKey(pos.UserID, ledger.SubTypeCollateral, quoteAssetID),
				CreditAccount: ledger.NewUserAccountKey(pos.UserID, ledger.SubTypeReserved, quoteAssetID),
				AssetID:       quoteAssetID,
				Amount:        delta,
				JournalType:   ledger.JournalTypeMarginRelease,
				Timestamp:     evt.Timestamp,
			})
		}
	}

	// Recompute margin for all positions in this market (may trigger liquidations)
	c.checkMarginForMarket(evt.Market, evt.Timestamp)

	return batch, nil
}

func (c *DeterministicCore) dispatchEvent(evt event.Event) (*ledger.Batch, error) {
	switch e := evt.(type) {
	case *event.DepositInitiated:
		return c.handleDepositInitiated(e)
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
	case *event.LiquidationCompleted:
		return c.handleLiquidationCompleted(e)
	case *event.RiskParamUpdate:
		return c.handleRiskParamUpdate(e)
	default:
		return nil, fmt.Errorf("unknown event type: %T", evt)
	}
}

// --- Snapshot Restore & Startup Methods ---

// SnapshotState holds the serializable in-memory state for restore.
// This mirrors persistence.SnapshotData but uses typed fields.
type SnapshotState struct {
	Sequence          int64
	StateHash         [32]byte
	PrevHash          [32]byte
	Balances          map[ledger.AccountKey]int64
	Positions         []*state.Position
	MarkPrices        map[string]*state.MarkPriceState
	FundingSnapshots  map[string]*state.FundingSnapshot
	FundingNextEpochs map[string]int64
	SequenceState     map[string]int64
	IdempotencyKeys   []string
}

// RestoreFromSnapshot restores the core's in-memory state from a snapshot.
// Per doc §11: on warm restart, load latest snapshot then replay events.
func (c *DeterministicCore) RestoreFromSnapshot(snap *SnapshotState) {
	// Restore sequence
	c.sequence = snap.Sequence + 1 // Next sequence to assign

	// Restore state hash chain
	c.hasher.SetPrevHash(snap.StateHash)

	// Restore balances
	for key, balance := range snap.Balances {
		c.balanceTracker.SetBalance(key, balance)
	}

	// Restore positions
	for _, pos := range snap.Positions {
		c.positionManager.SetPosition(pos)
	}

	// Restore mark prices
	for marketID, mp := range snap.MarkPrices {
		c.positionManager.RestoreMarkPrice(marketID, mp)
	}

	// Restore funding state
	for key, fs := range snap.FundingSnapshots {
		_ = key // key is "market:epoch", already encoded in FundingSnapshot
		c.fundingManager.RestoreSnapshot(fs)
	}
	for marketID, nextEpoch := range snap.FundingNextEpochs {
		c.fundingManager.RestoreNextEpoch(marketID, nextEpoch)
	}

	// Restore sequence validator state
	for partition, nextSeq := range snap.SequenceState {
		c.sequenceValidator.RestorePartition(partition, nextSeq)
	}

	// Restore journal generator sequence
	c.journalGen.SetSequence(snap.Sequence)
}

// WarmLRU loads recent idempotency keys into the LRU cache.
// Per doc §10: avoids cold-path DB lookups for recently processed events.
func (c *DeterministicCore) WarmLRU(keys []string) {
	c.idempotency.lru.WarmFromKeys(keys)
}

// GetSequence returns the current global sequence number.
func (c *DeterministicCore) GetSequence() int64 {
	return c.sequence
}

// GetStateHash returns the current state hash (chain tip).
func (c *DeterministicCore) GetStateHash() [32]byte {
	return c.hasher.GetPrevHash()
}

// CreateSnapshotState captures the current in-memory state for persistence.
func (c *DeterministicCore) CreateSnapshotState() *SnapshotState {
	return &SnapshotState{
		Sequence:          c.sequence - 1, // Last processed sequence
		StateHash:         c.hasher.GetPrevHash(),
		Balances:          c.balanceTracker.Snapshot(),
		Positions:         c.positionManager.GetAllPositions(),
		MarkPrices:        c.positionManager.GetAllMarkPrices(),
		FundingSnapshots:  c.fundingManager.GetAllSnapshots(),
		FundingNextEpochs: c.fundingManager.GetAllNextEpochs(),
		SequenceState:     c.sequenceValidator.GetAllPartitions(),
		IdempotencyKeys:   c.idempotency.lru.GetAllKeys(),
	}
}
