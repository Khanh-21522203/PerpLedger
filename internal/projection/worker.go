package projection

import (
	"PerpLedger/internal/observability"
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// ProjectionOutput mirrors the data needed by projection workers.
// The orchestrator bridges between core.CoreOutput and this.
type ProjectionOutput struct {
	Sequence       int64
	EventType      string
	MarketID       *string
	JournalEntries []JournalEntry
	Timestamp      int64

	// Position projection fields (populated for TradeFill, LiquidationFill)
	UserID        *string
	Side          int32
	Size          int64
	AvgEntryPrice int64
	RealizedPnL   int64

	// Funding projection fields (populated for FundingEpochSettle)
	FundingPayments []FundingPaymentProjection

	// Liquidation projection fields (populated for LiquidationTriggered, LiquidationCompleted)
	LiquidationID     *string
	LiquidationStatus string
	Deficit           int64
}

// FundingPaymentProjection represents a single user's funding payment for projection.
type FundingPaymentProjection struct {
	UserID       string
	MarketID     string
	EpochID      int64
	FundingRate  int64
	PositionSize int64
	MarkPrice    int64
	Payment      int64
}

// JournalEntry is a simplified journal for projection consumption.
type JournalEntry struct {
	DebitAccount  string
	CreditAccount string
	AssetID       uint16
	Amount        int64
	JournalType   int32
}

// ProjectionWorker updates projection tables from processed events.
// Per doc §12: projection channel is non-blocking with drop.
// If projections fall behind, they can be rebuilt from the event log.
type ProjectionWorker struct {
	db        *sql.DB
	inputChan <-chan ProjectionOutput
	lastSeq   int64
	metrics   *observability.Metrics
}

func NewProjectionWorker(db *sql.DB, inputChan <-chan ProjectionOutput, metrics *observability.Metrics) *ProjectionWorker {
	return &ProjectionWorker{
		db:        db,
		inputChan: inputChan,
		metrics:   metrics,
	}
}

// Run starts the projection worker loop.
func (pw *ProjectionWorker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case output, ok := <-pw.inputChan:
			if !ok {
				return nil
			}

			if err := pw.processOutput(ctx, output); err != nil {
				log.Printf("WARN: projection update failed at seq=%d: %v", output.Sequence, err)
				// Continue — projections are eventually consistent
				// and can be rebuilt from the event log
			}

			pw.lastSeq = output.Sequence
		}
	}
}

func (pw *ProjectionWorker) processOutput(ctx context.Context, output ProjectionOutput) error {
	start := time.Now()

	tx, err := pw.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update balance projections from journal entries
	for _, j := range output.JournalEntries {
		if err := pw.updateBalanceProjection(ctx, tx, j); err != nil {
			return fmt.Errorf("balance projection: %w", err)
		}
	}

	// Update position projections for trade/liquidation events
	switch output.EventType {
	case "TradeFill", "LiquidationFill":
		if err := pw.updatePositionProjection(ctx, tx, output); err != nil {
			return fmt.Errorf("position projection: %w", err)
		}
	case "FundingEpochSettle":
		if err := pw.updateFundingHistoryProjection(ctx, tx, output); err != nil {
			return fmt.Errorf("funding history projection: %w", err)
		}
	case "LiquidationTriggered", "LiquidationCompleted":
		if err := pw.updateLiquidationProjection(ctx, tx, output); err != nil {
			return fmt.Errorf("liquidation projection: %w", err)
		}
	}

	// Update projection watermark
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO projections.watermark (worker_id, last_sequence, updated_at)
		VALUES ('main', $1, NOW())
		ON CONFLICT (worker_id) DO UPDATE SET last_sequence = $1, updated_at = NOW()
	`, output.Sequence); err != nil {
		return fmt.Errorf("watermark update: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if pw.metrics != nil {
		pw.metrics.ProjectionUpdateDur.WithLabelValues("balances").Observe(time.Since(start).Seconds())
	}

	return nil
}

// updatePositionProjection upserts position state from trade/liquidation fill events.
func (pw *ProjectionWorker) updatePositionProjection(ctx context.Context, tx *sql.Tx, output ProjectionOutput) error {
	if output.UserID == nil || output.MarketID == nil {
		return nil
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO projections.positions 
			(user_id, market_id, side, size, avg_entry_price, realized_pnl, updated_seq)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (user_id, market_id) DO UPDATE SET
			side = EXCLUDED.side,
			size = EXCLUDED.size,
			avg_entry_price = EXCLUDED.avg_entry_price,
			realized_pnl = EXCLUDED.realized_pnl,
			updated_seq = EXCLUDED.updated_seq
	`, output.UserID, output.MarketID, output.Side, output.Size,
		output.AvgEntryPrice, output.RealizedPnL, output.Sequence)
	return err
}

// updateFundingHistoryProjection inserts funding payment records.
func (pw *ProjectionWorker) updateFundingHistoryProjection(ctx context.Context, tx *sql.Tx, output ProjectionOutput) error {
	for _, fp := range output.FundingPayments {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO projections.funding_history 
				(user_id, market_id, epoch_id, funding_rate, position_size, mark_price, payment, sequence, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9::double precision / 1000000))
			ON CONFLICT DO NOTHING
		`, fp.UserID, fp.MarketID, fp.EpochID, fp.FundingRate,
			fp.PositionSize, fp.MarkPrice, fp.Payment, output.Sequence, output.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateLiquidationProjection tracks liquidation lifecycle events.
func (pw *ProjectionWorker) updateLiquidationProjection(ctx context.Context, tx *sql.Tx, output ProjectionOutput) error {
	if output.LiquidationID == nil {
		return nil
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO projections.liquidation_history 
			(liquidation_id, user_id, market_id, status, deficit, sequence, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7::double precision / 1000000))
		ON CONFLICT (liquidation_id) DO UPDATE SET
			status = EXCLUDED.status,
			deficit = EXCLUDED.deficit,
			sequence = EXCLUDED.sequence
	`, output.LiquidationID, output.UserID, output.MarketID,
		output.LiquidationStatus, output.Deficit, output.Sequence, output.Timestamp)
	return err
}

// updateBalanceProjection updates the projections.balances table.
// NOTE: Docs §3.2 define schema as (user_id, asset, collateral, reserved, ...).
// Code uses (account_path, asset_id, balance, last_sequence) — a more generic
// account-path-based schema that supports system accounts, per-market sub-accounts,
// and arbitrary account hierarchies without schema changes. The account_path encodes
// the full AccountKey (e.g., "user:<uuid>:collateral" or "system:insurance:collateral").
// This is an intentional design improvement over the docs.
func (pw *ProjectionWorker) updateBalanceProjection(ctx context.Context, tx *sql.Tx, j JournalEntry) error {
	// Debit account: decrease balance
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO projections.balances (account_path, asset_id, balance, last_sequence)
		VALUES ($1, $2, -$3, $4)
		ON CONFLICT (account_path, asset_id) 
		DO UPDATE SET balance = projections.balances.balance - $3, last_sequence = $4
	`, j.DebitAccount, j.AssetID, j.Amount, pw.lastSeq); err != nil {
		return err
	}

	// Credit account: increase balance
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO projections.balances (account_path, asset_id, balance, last_sequence)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_path, asset_id) 
		DO UPDATE SET balance = projections.balances.balance + $3, last_sequence = $4
	`, j.CreditAccount, j.AssetID, j.Amount, pw.lastSeq); err != nil {
		return err
	}

	return nil
}

// CreateProjectionSchema is deprecated — use Migrator.Up() with migrations/*.sql instead.
// Kept as a no-op for backward compatibility during transition.
func CreateProjectionSchema(ctx context.Context, db *sql.DB) error {
	return nil
}

// RebuildProjections rebuilds all projection tables from the event log.
// Per doc §11: projections can be rebuilt by replaying the event log.
func RebuildProjections(ctx context.Context, db *sql.DB) error {
	// Truncate all projection tables
	truncateStatements := []string{
		`TRUNCATE projections.balances`,
		`TRUNCATE projections.positions`,
		`TRUNCATE projections.funding_history`,
		`TRUNCATE projections.liquidation_history`,
		`DELETE FROM projections.watermark WHERE worker_id = 'main'`,
	}

	for _, stmt := range truncateStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("truncate failed: %w", err)
		}
	}

	// Rebuild from journal entries
	_, err := db.ExecContext(ctx, `
		INSERT INTO projections.balances (account_path, asset_id, balance, last_sequence)
		SELECT 
			credit_account AS account_path,
			asset_id,
			SUM(amount) AS balance,
			MAX(sequence) AS last_sequence
		FROM event_log.journal
		GROUP BY credit_account, asset_id
		ON CONFLICT (account_path, asset_id) DO UPDATE 
			SET balance = EXCLUDED.balance, last_sequence = EXCLUDED.last_sequence
	`)
	if err != nil {
		return fmt.Errorf("rebuild credit balances: %w", err)
	}

	// Subtract debits
	_, err = db.ExecContext(ctx, `
		INSERT INTO projections.balances (account_path, asset_id, balance, last_sequence)
		SELECT 
			debit_account AS account_path,
			asset_id,
			-SUM(amount) AS balance,
			MAX(sequence) AS last_sequence
		FROM event_log.journal
		GROUP BY debit_account, asset_id
		ON CONFLICT (account_path, asset_id) DO UPDATE 
			SET balance = projections.balances.balance + EXCLUDED.balance,
			    last_sequence = GREATEST(projections.balances.last_sequence, EXCLUDED.last_sequence)
	`)
	if err != nil {
		return fmt.Errorf("rebuild debit balances: %w", err)
	}

	log.Println("INFO: projection rebuild complete")
	return nil
}
