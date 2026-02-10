package ledger

import (
	"fmt"

	"github.com/google/uuid"
)

// AccountScope represents the top-level account namespace
type AccountScope uint8

const (
	AccountScopeUser AccountScope = iota
	AccountScopeSystem
	AccountScopeExternal
)

// AccountSubType represents the account purpose
type AccountSubType uint8

const (
	// User sub-types
	SubTypeCollateral AccountSubType = iota
	SubTypeReserved
	SubTypePendingDeposit
	SubTypePendingWithdrawal
	SubTypeFundingAccrual
	SubTypePnL

	// System sub-types
	SubTypeSystemFees
	SubTypeSystemFundingPool
	SubTypeSystemInsuranceFund
	SubTypeSystemPendingDeposits
	SubTypeSystemPendingWithdrawals
	SubTypeSystemSocializedLoss

	// External sub-types
	SubTypeExternalDeposits
	SubTypeExternalWithdrawals
)

// AssetID maps asset strings to numeric IDs for performance
type AssetID uint16

var (
	assetToID = map[string]AssetID{
		"USDT": 1,
		"USDC": 2,
		"BTC":  3,
		"ETH":  4,
	}
	idToAsset = map[AssetID]string{
		1: "USDT",
		2: "USDC",
		3: "BTC",
		4: "ETH",
	}
)

func GetAssetID(asset string) (AssetID, bool) {
	id, ok := assetToID[asset]
	return id, ok
}

func GetAssetName(id AssetID) (string, bool) {
	name, ok := idToAsset[id]
	return name, ok
}

// AccountKey is the in-memory key for balance tracking (21 bytes, cache-friendly)
type AccountKey struct {
	Scope    AccountScope
	EntityID [16]byte // UUID for users, hash for system accounts
	SubType  AccountSubType
	AssetID  AssetID
}

// NewUserAccountKey creates a key for user accounts
func NewUserAccountKey(userID uuid.UUID, subType AccountSubType, assetID AssetID) AccountKey {
	return AccountKey{
		Scope:    AccountScopeUser,
		EntityID: userID,
		SubType:  subType,
		AssetID:  assetID,
	}
}

// NewSystemAccountKey creates a key for system accounts
func NewSystemAccountKey(name string, subType AccountSubType, assetID AssetID) AccountKey {
	var entityID [16]byte
	// Hash the name into 16 bytes
	copy(entityID[:], []byte(name))
	return AccountKey{
		Scope:    AccountScopeSystem,
		EntityID: entityID,
		SubType:  subType,
		AssetID:  assetID,
	}
}

// NewExternalAccountKey creates a key for external boundary accounts
func NewExternalAccountKey(subType AccountSubType, assetID AssetID) AccountKey {
	return AccountKey{
		Scope:   AccountScopeExternal,
		SubType: subType,
		AssetID: assetID,
	}
}

// AccountPath returns the string representation for storage/logging
func (k AccountKey) AccountPath() string {
	assetName, _ := GetAssetName(k.AssetID)

	switch k.Scope {
	case AccountScopeUser:
		uid := uuid.UUID(k.EntityID)
		return fmt.Sprintf("user:%s:%s:%s", uid.String(), k.subTypeName(), assetName)
	case AccountScopeSystem:
		return fmt.Sprintf("system:%s:%s", k.subTypeName(), assetName)
	case AccountScopeExternal:
		return fmt.Sprintf("external:%s:%s", k.subTypeName(), assetName)
	}
	return "unknown"
}

func (k AccountKey) subTypeName() string {
	switch k.SubType {
	case SubTypeCollateral:
		return "collateral"
	case SubTypeReserved:
		return "reserved"
	case SubTypePendingDeposit:
		return "pending_deposit"
	case SubTypePendingWithdrawal:
		return "pending_withdrawal"
	case SubTypeFundingAccrual:
		return "funding_accrual"
	case SubTypePnL:
		return "pnl"
	case SubTypeSystemFees:
		return "fees"
	case SubTypeSystemFundingPool:
		return "funding_pool"
	case SubTypeSystemInsuranceFund:
		return "insurance_fund"
	case SubTypeExternalDeposits:
		return "deposits"
	case SubTypeExternalWithdrawals:
		return "withdrawals"
	default:
		return "unknown"
	}
}
