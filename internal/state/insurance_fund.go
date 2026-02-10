package state

// InsuranceFund tracks the insurance fund balance and handles bankruptcy coverage.
// Per doc §9: when a liquidation results in a deficit (negative equity after full close),
// the insurance fund covers the shortfall. If the insurance fund is insufficient,
// the system escalates to ADL (Auto-Deleverage).
type InsuranceFund struct {
	// Balance is tracked via the ledger (system:insurance_fund:collateral account).
	// This struct provides helper methods for insurance fund operations.
}

func NewInsuranceFund() *InsuranceFund {
	return &InsuranceFund{}
}

// CanCoverDeficit checks if the insurance fund has enough balance to cover a deficit.
// The actual balance is read from the BalanceTracker via the system account key.
func (f *InsuranceFund) CanCoverDeficit(fundBalance int64, deficit int64) bool {
	return fundBalance >= deficit
}

// ComputeCoverage returns how much the insurance fund can cover.
// If the fund is insufficient, returns the partial amount and the remaining deficit.
func (f *InsuranceFund) ComputeCoverage(fundBalance int64, deficit int64) (covered int64, remaining int64) {
	if fundBalance >= deficit {
		return deficit, 0
	}
	return fundBalance, deficit - fundBalance
}
