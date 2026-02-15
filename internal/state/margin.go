package state

import (
	"PerpLedger/internal/ledger"
	"math"

	"github.com/google/uuid"
)

// MarginCalculator computes cross-margin metrics.
// It uses an interface for balanceTracker to avoid a direct import cycle
// while still accepting *ledger.BalanceTracker.
type MarginCalculator struct {
	positionMgr    *PositionManager
	balanceTracker interface {
		GetUserAvailableBalance(uuid.UUID, ledger.AssetID) int64
		GetUserReservedBalance(uuid.UUID, ledger.AssetID) int64
	}
	riskParamsMgr *RiskParamsManager
}

func NewMarginCalculator(
	pm *PositionManager,
	bt interface {
		GetUserAvailableBalance(uuid.UUID, ledger.AssetID) int64
		GetUserReservedBalance(uuid.UUID, ledger.AssetID) int64
	},
	rpm *RiskParamsManager,
) *MarginCalculator {
	return &MarginCalculator{
		positionMgr:    pm,
		balanceTracker: bt,
		riskParamsMgr:  rpm,
	}
}

// ComputeEffectiveCollateral returns available + total unrealized PnL (with haircuts)
func (mc *MarginCalculator) ComputeEffectiveCollateral(
	userID uuid.UUID,
	quoteAssetID ledger.AssetID,
) int64 {
	var effectiveCollateral int64

	// Contribution from quote asset (no haircut, no price conversion)
	// Per flow margin-computation-flowchart: effective collateral includes
	// available (collateral) + reserved (margin-locked) balances.
	// Reserved margin still belongs to the user for margin fraction computation.
	available := mc.balanceTracker.GetUserAvailableBalance(userID, quoteAssetID)
	reserved := mc.balanceTracker.GetUserReservedBalance(userID, quoteAssetID)
	effectiveCollateral += available + reserved

	// TODO: Multi-asset collateral with haircuts (post-MVP)
	// For MVP, only USDT collateral
	// Example for future:
	// for each asset a where user has balance:
	//     asset_balance = available_balance(user, a)
	//     if a == quote_asset:
	//         effective_collateral += asset_balance
	//     else:
	//         asset_price = latest_mark_price(a_to_quote_market)
	//         haircut_factor = 1_000_000 - haircut(a)  // e.g., 950_000 for 5%
	//         raw = asset_balance * asset_price * haircut_factor
	//         contribution = round_half_even(raw / (asset_price_scale * 1_000_000))
	//         effective_collateral += contribution

	// Add unrealized PnL from all positions
	unrealizedPnL := mc.positionMgr.ComputeTotalUnrealizedPnL(userID)
	effectiveCollateral += unrealizedPnL

	return effectiveCollateral
}

// ComputeTotalNotional sums notional across all user positions
func (mc *MarginCalculator) ComputeTotalNotional(userID uuid.UUID) int64 {
	return mc.positionMgr.ComputeTotalNotional(userID)
}

// ComputeTotalMM calculates total maintenance margin
func (mc *MarginCalculator) ComputeTotalMM(userID uuid.UUID) int64 {
	positions := mc.positionMgr.GetUserPositions(userID)

	var totalMM int64

	for _, pos := range positions {
		if pos.IsFlat() {
			continue
		}

		params, ok := mc.riskParamsMgr.GetRiskParams(pos.MarketID)
		if !ok {
			// No params - skip (should not happen)
			continue
		}

		notional, err := mc.positionMgr.ComputePositionNotional(pos)
		if err != nil {
			continue
		}

		// mm_contribution = notional * mm_fraction / 1_000_000
		mmContribution := notional * params.MMFraction / 1_000_000
		totalMM += mmContribution
	}

	return totalMM
}

// ComputeTotalIM calculates total initial margin
func (mc *MarginCalculator) ComputeTotalIM(userID uuid.UUID) int64 {
	positions := mc.positionMgr.GetUserPositions(userID)

	var totalIM int64

	for _, pos := range positions {
		if pos.IsFlat() {
			continue
		}

		params, ok := mc.riskParamsMgr.GetRiskParams(pos.MarketID)
		if !ok {
			continue
		}

		notional, err := mc.positionMgr.ComputePositionNotional(pos)
		if err != nil {
			continue
		}

		// im_contribution = notional * im_fraction / 1_000_000
		imContribution := notional * params.IMFraction / 1_000_000
		totalIM += imContribution
	}

	return totalIM
}

// ComputeMarginFraction returns effective_collateral / total_notional
func (mc *MarginCalculator) ComputeMarginFraction(
	userID uuid.UUID,
	quoteAssetID ledger.AssetID,
) int64 {
	effectiveCollateral := mc.ComputeEffectiveCollateral(userID, quoteAssetID)
	totalNotional := mc.ComputeTotalNotional(userID)

	if totalNotional == 0 {
		// Per flow margin-computation-flowchart: no positions → margin_fraction = MAX_INT
		// This ensures the user is always Healthy when they have no exposure.
		return math.MaxInt64
	}

	// margin_fraction = effective / notional (as fixed-point percentage)
	// Result scale: same as margin fraction scale (1_000_000)
	fraction := effectiveCollateral * 1_000_000 / totalNotional

	return fraction
}

// CheckMarginHealth returns margin status for user
func (mc *MarginCalculator) CheckMarginHealth(
	userID uuid.UUID,
	quoteAssetID ledger.AssetID,
) MarginStatus {
	marginFraction := mc.ComputeMarginFraction(userID, quoteAssetID)

	// Get the strictest MM/IM across all positions
	positions := mc.positionMgr.GetUserPositions(userID)
	var maxMMFraction, maxIMFraction int64

	for _, pos := range positions {
		if pos.IsFlat() {
			continue
		}

		params, ok := mc.riskParamsMgr.GetRiskParams(pos.MarketID)
		if !ok {
			continue
		}

		if params.MMFraction > maxMMFraction {
			maxMMFraction = params.MMFraction
		}
		if params.IMFraction > maxIMFraction {
			maxIMFraction = params.IMFraction
		}
	}

	// Check status
	if marginFraction < maxMMFraction {
		return MarginStatusLiquidatable
	}
	if marginFraction < maxIMFraction {
		return MarginStatusAtRisk
	}
	return MarginStatusHealthy
}

// MarginStatus represents user's margin health
type MarginStatus int

const (
	MarginStatusHealthy MarginStatus = iota
	MarginStatusAtRisk
	MarginStatusLiquidatable
)

func (ms MarginStatus) String() string {
	switch ms {
	case MarginStatusHealthy:
		return "Healthy"
	case MarginStatusAtRisk:
		return "AtRisk"
	case MarginStatusLiquidatable:
		return "Liquidatable"
	default:
		return "Unknown"
	}
}
