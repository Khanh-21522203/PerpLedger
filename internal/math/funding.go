// internal/math/funding.go (NEW)
package math

import (
	"math/big"
	"sort"
)

// ComputeFundingPayment calculates funding payment for a position
// Returns: payment amount (positive = user pays, negative = user receives)
func ComputeFundingPayment(
	fundingRate int64, // Rate scale: 100_000_000
	positionSize int64, // Quantity scale: 1_000_000
	markPrice int64, // Price scale: 100
	sideSign int64, // +1 for long, -1 for short
) int64 {
	// raw = fundingRate * positionSize * markPrice
	temp1 := MultiplyInt128(fundingRate, positionSize)
	temp2 := getInt128()
	temp2.Mul(temp1, big.NewInt(markPrice))

	// Convert to quote scale
	// intermediate scale = R_s * Q_s * P_s = 100_000_000 * 1_000_000 * 100 = 10^16
	// target scale = 1_000_000 (USDT)
	denominator := int64(10_000_000_000) // 10^16 / 10^6 = 10^10

	payment := DivideInt128(temp2, denominator, RoundHalfEven)

	putInt128(temp1)
	putInt128(temp2)

	// Apply side sign
	// Long + positive rate = pays (positive payment)
	// Short + positive rate = receives (negative payment)
	return payment * sideSign
}

// FundingSettlement represents computed funding for all positions
type FundingSettlement struct {
	MarketID    string
	EpochID     int64
	FundingRate int64
	MarkPrice   int64
	Payments    []UserPayment
	RoundingFee int64 // Residual to post to fees account
}

type UserPayment struct {
	UserID  [16]byte // UUID binary
	Payment int64    // Signed: positive = pays, negative = receives
}

// ComputeFundingSettlement calculates funding for all positions in a market
func ComputeFundingSettlement(
	marketID string,
	epochID int64,
	fundingRate int64,
	markPrice int64,
	positions []PositionForFunding,
) *FundingSettlement {
	// Sort positions by user_id for deterministic ordering
	sort.Slice(positions, func(i, j int) bool {
		for k := 0; k < 16; k++ {
			if positions[i].UserID[k] != positions[j].UserID[k] {
				return positions[i].UserID[k] < positions[j].UserID[k]
			}
		}
		return false
	})

	payments := make([]UserPayment, 0, len(positions))
	var totalPaid, totalReceived int64

	for _, pos := range positions {
		if pos.Size == 0 {
			continue // Skip flat positions
		}

		payment := ComputeFundingPayment(
			fundingRate,
			pos.Size,
			markPrice,
			pos.SideSign,
		)

		if payment != 0 {
			payments = append(payments, UserPayment{
				UserID:  pos.UserID,
				Payment: payment,
			})

			if payment > 0 {
				totalPaid += payment
			} else {
				totalReceived += -payment
			}
		}
	}

	// Calculate rounding residual
	roundingFee := totalPaid - totalReceived

	return &FundingSettlement{
		MarketID:    marketID,
		EpochID:     epochID,
		FundingRate: fundingRate,
		MarkPrice:   markPrice,
		Payments:    payments,
		RoundingFee: roundingFee,
	}
}

type PositionForFunding struct {
	UserID   [16]byte
	Size     int64
	SideSign int64
}
