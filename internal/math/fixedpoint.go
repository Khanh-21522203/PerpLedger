package math

import (
	"math/big"
	"sync"
)

// DecimalConfig defines fixed-point precision
type DecimalConfig struct {
	DecimalPrecision int   // Number of decimal places
	Scale            int64 // 10^DecimalPrecision
}

var (
	// Standard configs
	PriceConfig    = DecimalConfig{DecimalPrecision: 2, Scale: 100}         // 0.01
	QuantityConfig = DecimalConfig{DecimalPrecision: 6, Scale: 1_000_000}   // 0.000001
	QuoteConfig    = DecimalConfig{DecimalPrecision: 6, Scale: 1_000_000}   // 0.000001 USDT
	RateConfig     = DecimalConfig{DecimalPrecision: 8, Scale: 100_000_000} // 0.00000001 (funding rate)
)

// TRICKY: All monetary multiplications use big.Int to prevent int64 overflow.
// For example, price(100) * quantity(1_000_000) * rate(100_000_000) can exceed 2^63.
// We pool big.Int instances to reduce GC pressure in the hot path (per doc §12).
var int128Pool = &sync.Pool{
	New: func() interface{} {
		return new(big.Int)
	},
}

func getInt128() *big.Int {
	return int128Pool.Get().(*big.Int)
}

func putInt128(v *big.Int) {
	v.SetInt64(0) // Clear before returning to pool
	int128Pool.Put(v)
}

// MultiplyInt128 performs a * b using int128 to prevent overflow
func MultiplyInt128(a, b int64) *big.Int {
	result := getInt128()
	result.Mul(big.NewInt(a), big.NewInt(b))
	return result
}

// DivideInt128 performs numerator / denominator with rounding
func DivideInt128(numerator *big.Int, denominator int64, roundingMode RoundingMode) int64 {
	denom := big.NewInt(denominator)
	quotient := getInt128()
	remainder := getInt128()

	quotient.DivMod(numerator, denom, remainder)

	// Apply rounding
	result := quotient.Int64()

	if roundingMode == RoundHalfEven {
		// TRICKY: Banker's rounding (round-half-to-even) ensures deterministic
		// results with no systematic bias. This is critical for funding settlement
		// where rounding residuals must be accounted for (posted to fees account).
		half := big.NewInt(denominator / 2)
		cmp := remainder.Cmp(half)

		if cmp > 0 {
			result++
		} else if cmp == 0 && denominator%2 == 0 {
			// Exact half: round to nearest even to avoid bias
			if result%2 != 0 {
				result++
			}
		}
	}

	putInt128(quotient)
	putInt128(remainder)

	return result
}

type RoundingMode int

const (
	RoundHalfEven RoundingMode = iota // Banker's rounding (default)
	RoundDown
	RoundUp
)

// ComputeAvgEntryPrice calculates weighted average entry price
func ComputeAvgEntryPrice(oldSize, oldAvgEntry, fillQty, fillPrice int64) int64 {
	if oldSize == 0 {
		return fillPrice
	}

	// numerator = oldSize * oldAvgEntry + fillQty * fillPrice
	term1 := MultiplyInt128(oldSize, oldAvgEntry)
	term2 := MultiplyInt128(fillQty, fillPrice)
	numerator := getInt128()
	numerator.Add(term1, term2)

	// denominator = oldSize + fillQty
	denominator := oldSize + fillQty

	// result = numerator / denominator (with banker's rounding)
	result := DivideInt128(numerator, denominator, RoundHalfEven)

	putInt128(term1)
	putInt128(term2)
	putInt128(numerator)

	return result
}

// ComputeRealizedPnL calculates PnL for position close.
// Formula: sideSign * (fillPrice - avgEntryPrice) * closeQty * quoteScale / (priceScale * qtyScale)
// The scale conversion normalizes from (price_scale × qty_scale) to quote_scale.
func ComputeRealizedPnL(
	sideSign int64, // +1 for long, -1 for short
	fillPrice int64, // Price scale
	avgEntryPrice int64, // Price scale
	closeQty int64, // Quantity scale
	priceScale int64, // From PriceConfig
	qtyScale int64, // From QuantityConfig
	quoteScale int64, // From QuoteConfig
) int64 {
	// price_diff = fillPrice - avgEntryPrice
	priceDiff := fillPrice - avgEntryPrice

	// raw_pnl = sideSign * priceDiff * closeQty
	temp := MultiplyInt128(sideSign*priceDiff, closeQty)

	// Convert to quote precision: raw_pnl * quoteScale / (priceScale * qtyScale)
	temp.Mul(temp, big.NewInt(quoteScale))
	denominator := priceScale * qtyScale

	result := DivideInt128(temp, denominator, RoundHalfEven)

	putInt128(temp)

	return result
}

// ComputeUnrealizedPnL calculates unrealized PnL
func ComputeUnrealizedPnL(
	sideSign int64,
	markPrice int64,
	avgEntryPrice int64,
	positionSize int64,
	priceScale int64,
	qtyScale int64,
	quoteScale int64,
) int64 {
	return ComputeRealizedPnL(
		sideSign,
		markPrice,
		avgEntryPrice,
		positionSize,
		priceScale,
		qtyScale,
		quoteScale,
	)
}

// ComputeNotional calculates position notional value
func ComputeNotional(
	positionSize int64,
	markPrice int64,
	priceScale int64,
	qtyScale int64,
	quoteScale int64,
) int64 {
	// raw_notional = positionSize * markPrice
	raw := MultiplyInt128(positionSize, markPrice)

	// Convert to quote scale
	raw.Mul(raw, big.NewInt(quoteScale))
	denominator := priceScale * qtyScale

	result := DivideInt128(raw, denominator, RoundHalfEven)

	putInt128(raw)

	return result
}
