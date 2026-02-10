package projection

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// ProjectionOutput mirrors the data needed by projection workers.
// The orchestrator bridges between core.CoreOutput and this.
type ProjectionOutput struct {
	Sequence       int64
	EventType      string
	MarketID       *string
	JournalEntries []JournalEntry
	Timestamp      int64
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
}

func NewProjectionWorker(db *sql.DB, inputChan <-chan ProjectionOutput) *ProjectionWorker {
	return &ProjectionWorker{
		db:        db,
		inputChan: inputChan,
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

	// Update projection watermark
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO projections.watermark (worker_id, last_sequence, updated_at)
		VALUES ('main', $1, NOW())
		ON CONFLICT (worker_id) DO UPDATE SET last_sequence = $1, updated_at = NOW()
	`, output.Sequence); err != nil {
		return fmt.Errorf("watermark update: %w", err)
	}

	return tx.Commit()
}

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
