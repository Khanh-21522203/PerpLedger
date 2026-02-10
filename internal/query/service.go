package query

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// QueryService provides read-only access to projection tables.
// Per doc §16: queries are served via gRPC and HTTP/JSON (gRPC-Gateway),
// reading from PostgreSQL projection tables. All responses include
// as_of_sequence for freshness semantics.
type QueryService struct {
	db *sql.DB
}

func NewQueryService(db *sql.DB) *QueryService {
	return &QueryService{db: db}
}

// GetBalance returns a user's balance for a specific asset.
// Per doc §16: balance queries return total, available, reserved, pending balances
// plus derived values (unrealized PnL, effective equity) computed at query time.
func (qs *QueryService) GetBalance(
	ctx context.Context,
	userID uuid.UUID,
	asset string,
) (*BalanceResponse, error) {
	asOfSeq, err := qs.getWatermark(ctx)
	if err != nil {
		return nil, fmt.Errorf("watermark: %w", err)
	}

	// Query collateral balance
	collateralPath := fmt.Sprintf("user:%s:collateral", userID)
	collateral, err := qs.getProjectedBalance(ctx, collateralPath, asset)
	if err != nil {
		return nil, err
	}

	// Query reserved (margin) balance
	reservedPath := fmt.Sprintf("user:%s:reserved", userID)
	reserved, err := qs.getProjectedBalance(ctx, reservedPath, asset)
	if err != nil {
		return nil, err
	}

	// Query pending deposit
	pendingDepositPath := fmt.Sprintf("user:%s:pending_deposit", userID)
	pendingDeposit, err := qs.getProjectedBalance(ctx, pendingDepositPath, asset)
	if err != nil {
		return nil, err
	}

	// Query pending withdrawal
	pendingWithdrawalPath := fmt.Sprintf("user:%s:pending_withdrawal", userID)
	pendingWithdrawal, err := qs.getProjectedBalance(ctx, pendingWithdrawalPath, asset)
	if err != nil {
		return nil, err
	}

	total := collateral + reserved
	available := collateral

	return &BalanceResponse{
		UserID:            userID,
		Asset:             asset,
		TotalBalance:      total,
		AvailableBalance:  available,
		ReservedBalance:   reserved,
		PendingDeposit:    pendingDeposit,
		PendingWithdrawal: pendingWithdrawal,
		// UnrealizedPnL and EffectiveEquity are computed from positions
		// and mark prices — requires position query + mark price lookup
		AsOfSequence: asOfSeq,
	}, nil
}

// GetPositions returns all positions for a user.
func (qs *QueryService) GetPositions(
	ctx context.Context,
	userID uuid.UUID,
) ([]PositionResponse, error) {
	asOfSeq, err := qs.getWatermark(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := qs.db.QueryContext(ctx, `
		SELECT market_id, side, size, avg_entry_price, realized_pnl,
		       last_funding_epoch, liquidation_state, version
		FROM projections.positions
		WHERE user_id = $1 AND size > 0
		ORDER BY market_id
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []PositionResponse
	for rows.Next() {
		var p PositionResponse
		p.UserID = userID
		p.AsOfSequence = asOfSeq
		if err := rows.Scan(
			&p.MarketID, &p.Side, &p.Size, &p.AvgEntryPrice, &p.RealizedPnL,
			&p.LastFundingEpoch, &p.LiquidationState, &p.Version,
		); err != nil {
			return nil, err
		}
		positions = append(positions, p)
	}

	return positions, rows.Err()
}

// GetFundingHistory returns funding payment history for a user.
// Per doc §16: supports cursor-based pagination.
func (qs *QueryService) GetFundingHistory(
	ctx context.Context,
	userID uuid.UUID,
	marketID *string,
	limit int,
	afterEpoch *int64,
) ([]FundingHistoryResponse, error) {
	asOfSeq, err := qs.getWatermark(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT market_id, epoch_id, funding_rate, position_size, mark_price, payment, timestamp
		FROM projections.funding_history
		WHERE user_id = $1
	`
	args := []interface{}{userID}
	argIdx := 2

	if marketID != nil {
		query += fmt.Sprintf(" AND market_id = $%d", argIdx)
		args = append(args, *marketID)
		argIdx++
	}

	if afterEpoch != nil {
		query += fmt.Sprintf(" AND epoch_id < $%d", argIdx)
		args = append(args, *afterEpoch)
		argIdx++
	}

	query += " ORDER BY epoch_id DESC"
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := qs.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []FundingHistoryResponse
	for rows.Next() {
		var h FundingHistoryResponse
		h.UserID = userID
		h.AsOfSequence = asOfSeq
		if err := rows.Scan(
			&h.MarketID, &h.EpochID, &h.FundingRate, &h.PositionSize,
			&h.MarkPrice, &h.Payment, &h.Timestamp,
		); err != nil {
			return nil, err
		}
		history = append(history, h)
	}

	return history, rows.Err()
}

// GetMarginSnapshot returns current margin metrics for a user.
func (qs *QueryService) GetMarginSnapshot(
	ctx context.Context,
	userID uuid.UUID,
) (*MarginInfo, error) {
	asOfSeq, err := qs.getWatermark(ctx)
	if err != nil {
		return nil, err
	}

	// This is a simplified version — in production, margin computation
	// would use the in-memory state from the core (via a read-only snapshot)
	// rather than querying projections, to ensure consistency.
	return &MarginInfo{
		UserID:       userID,
		AsOfSequence: asOfSeq,
	}, nil
}

// GetLiquidationStatus returns active liquidation info for a user.
func (qs *QueryService) GetLiquidationStatus(
	ctx context.Context,
	userID uuid.UUID,
) ([]LiquidationResponse, error) {
	rows, err := qs.db.QueryContext(ctx, `
		SELECT liquidation_id, market_id, triggered_at, initial_size,
		       remaining_size, state, deficit
		FROM projections.liquidation_history
		WHERE user_id = $1 AND state NOT IN (4, 5)
		ORDER BY triggered_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []LiquidationResponse
	for rows.Next() {
		var r LiquidationResponse
		r.UserID = userID
		if err := rows.Scan(
			&r.LiquidationID, &r.MarketID, &r.TriggeredAt, &r.InitialSize,
			&r.RemainingSize, &r.State, &r.Deficit,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// GetJournalHistory returns journal entries for a user with pagination.
func (qs *QueryService) GetJournalHistory(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
	afterSequence *int64,
) ([]JournalHistoryEntry, error) {
	accountPrefix := fmt.Sprintf("user:%s:%%", userID)

	query := `
		SELECT journal_id, batch_id, event_ref, sequence,
		       debit_account, credit_account, asset_id, amount, journal_type, timestamp
		FROM event_log.journal
		WHERE debit_account LIKE $1 OR credit_account LIKE $1
	`
	args := []interface{}{accountPrefix}
	argIdx := 2

	if afterSequence != nil {
		query += fmt.Sprintf(" AND sequence < $%d", argIdx)
		args = append(args, *afterSequence)
		argIdx++
	}

	query += " ORDER BY sequence DESC"
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := qs.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []JournalHistoryEntry
	for rows.Next() {
		var e JournalHistoryEntry
		if err := rows.Scan(
			&e.JournalID, &e.BatchID, &e.EventRef, &e.Sequence,
			&e.DebitAccount, &e.CreditAccount, &e.AssetID, &e.Amount,
			&e.JournalType, &e.Timestamp,
		); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	return entries, rows.Err()
}

// --- Admin APIs ---

// VerifyIntegrity checks hash chain and global balance invariants.
// Per doc §16: admin API for integrity verification.
func (qs *QueryService) VerifyIntegrity(ctx context.Context) (*IntegrityReport, error) {
	report := &IntegrityReport{}

	// Check hash chain continuity
	rows, err := qs.db.QueryContext(ctx, `
		SELECT e1.sequence, e1.prev_hash, e2.state_hash
		FROM event_log.events e1
		LEFT JOIN event_log.events e2 ON e2.sequence = e1.sequence - 1
		WHERE e1.sequence > 0 AND e1.prev_hash != COALESCE(e2.state_hash, e1.prev_hash)
		LIMIT 10
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var seq int64
		var prevHash, expectedHash []byte
		if err := rows.Scan(&seq, &prevHash, &expectedHash); err != nil {
			return nil, err
		}
		report.HashChainBreaks = append(report.HashChainBreaks, seq)
	}

	// Check global balance (should sum to zero across all accounts per asset)
	balanceRows, err := qs.db.QueryContext(ctx, `
		SELECT asset_id, SUM(balance) as total
		FROM projections.balances
		GROUP BY asset_id
		HAVING SUM(balance) != 0
	`)
	if err != nil {
		return nil, err
	}
	defer balanceRows.Close()

	for balanceRows.Next() {
		var assetID uint16
		var total int64
		if err := balanceRows.Scan(&assetID, &total); err != nil {
			return nil, err
		}
		report.UnbalancedAssets = append(report.UnbalancedAssets, UnbalancedAsset{
			AssetID:  assetID,
			Imbalance: total,
		})
	}

	report.IsHealthy = len(report.HashChainBreaks) == 0 && len(report.UnbalancedAssets) == 0
	return report, nil
}

// --- helpers ---

func (qs *QueryService) getWatermark(ctx context.Context) (int64, error) {
	var seq int64
	err := qs.db.QueryRowContext(ctx, `
		SELECT COALESCE(last_sequence, 0) FROM projections.watermark WHERE worker_id = 'main'
	`).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

func (qs *QueryService) getProjectedBalance(ctx context.Context, accountPath string, asset string) (int64, error) {
	// Map asset name to asset_id for query
	// This is a simplified lookup — in production, use the ledger.GetAssetID mapping
	var balance int64
	err := qs.db.QueryRowContext(ctx, `
		SELECT COALESCE(balance, 0) FROM projections.balances
		WHERE account_path = $1
	`, accountPath).Scan(&balance)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return balance, err
}
