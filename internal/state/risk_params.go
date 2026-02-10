// internal/state/risk_params.go (NEW)
package state

// RiskParams defines margin requirements per market
type RiskParams struct {
	MarketID     string
	IMFraction   int64 // Initial Margin (decimal_precision=6, scale=1_000_000)
	MMFraction   int64 // Maintenance Margin (decimal_precision=6, scale=1_000_000)
	MaxLeverage  int64 // Derived: 1_000_000 / IMFraction
	TickSize     int64 // Minimum price increment
	LotSize      int64 // Minimum quantity increment
	EffectiveSeq int64 // Sequence at which params take effect
}

// Haircut defines collateral discount for non-quote assets
type Haircut struct {
	Asset   string
	Haircut int64 // (decimal_precision=6, scale=1_000_000; e.g., 50_000 = 5%)
}

var (
	// Default risk params (MVP)
	DefaultRiskParams = map[string]*RiskParams{
		"BTC-USDT-PERP": {
			MarketID:     "BTC-USDT-PERP",
			IMFraction:   100_000, // 10%
			MMFraction:   50_000,  // 5%
			MaxLeverage:  10,
			TickSize:     100,   // 0.01 USDT
			LotSize:      1_000, // 0.001 BTC
			EffectiveSeq: 0,
		},
		"ETH-USDT-PERP": {
			MarketID:     "ETH-USDT-PERP",
			IMFraction:   100_000,
			MMFraction:   50_000,
			MaxLeverage:  10,
			TickSize:     100,
			LotSize:      1_000,
			EffectiveSeq: 0,
		},
	}

	// Default haircuts (MVP)
	DefaultHaircuts = map[string]*Haircut{
		"USDT": {Asset: "USDT", Haircut: 0},      // 0% haircut
		"BTC":  {Asset: "BTC", Haircut: 50_000},  // 5% haircut
		"ETH":  {Asset: "ETH", Haircut: 100_000}, // 10% haircut
	}
)

// RiskParamsManager manages risk parameters
type RiskParamsManager struct {
	params   map[string]*RiskParams
	haircuts map[string]*Haircut
}

func NewRiskParamsManager() *RiskParamsManager {
	// Initialize with defaults
	params := make(map[string]*RiskParams)
	for k, v := range DefaultRiskParams {
		params[k] = v
	}

	haircuts := make(map[string]*Haircut)
	for k, v := range DefaultHaircuts {
		haircuts[k] = v
	}

	return &RiskParamsManager{
		params:   params,
		haircuts: haircuts,
	}
}

func (rpm *RiskParamsManager) GetRiskParams(marketID string) (*RiskParams, bool) {
	params, ok := rpm.params[marketID]
	return params, ok
}

func (rpm *RiskParamsManager) GetHaircut(asset string) int64 {
	if haircut, ok := rpm.haircuts[asset]; ok {
		return haircut.Haircut
	}
	return 0 // Default: no haircut
}

func (rpm *RiskParamsManager) UpdateRiskParams(params *RiskParams) {
	rpm.params[params.MarketID] = params
}
