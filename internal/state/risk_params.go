package state

import "fmt"

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

// ValidateRiskParams checks that risk parameters are within valid ranges.
// Per flow risk-param-update-flowchart: mm > 0, im > mm, im < 1_000_000,
// max_leverage > 0, tick_size > 0, lot_size > 0.
func ValidateRiskParams(params *RiskParams) error {
	if params.MMFraction <= 0 {
		return fmt.Errorf("mm_fraction must be > 0, got %d", params.MMFraction)
	}
	if params.IMFraction <= params.MMFraction {
		return fmt.Errorf("im_fraction (%d) must be > mm_fraction (%d)", params.IMFraction, params.MMFraction)
	}
	if params.IMFraction >= 1_000_000 {
		return fmt.Errorf("im_fraction must be < 1_000_000, got %d", params.IMFraction)
	}
	if params.MaxLeverage <= 0 {
		return fmt.Errorf("max_leverage must be > 0, got %d", params.MaxLeverage)
	}
	if params.TickSize <= 0 {
		return fmt.Errorf("tick_size must be > 0, got %d", params.TickSize)
	}
	if params.LotSize <= 0 {
		return fmt.Errorf("lot_size must be > 0, got %d", params.LotSize)
	}
	return nil
}

func (rpm *RiskParamsManager) UpdateRiskParams(params *RiskParams) error {
	if err := ValidateRiskParams(params); err != nil {
		return fmt.Errorf("invalid risk params for %s: %w", params.MarketID, err)
	}
	rpm.params[params.MarketID] = params
	return nil
}
