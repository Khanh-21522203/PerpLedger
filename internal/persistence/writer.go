package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Executor abstracts *sql.DB and *sql.Tx so writers can operate within a transaction.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// EventLogWriter writes events and journals to Postgres using COPY protocol.
// Per doc §11 / flow persistence-worker-batching-flowchart: the persistence
// worker uses Postgres COPY protocol for high throughput batch writes.
type EventLogWriter struct {
	db           *sql.DB
	batchSize    int
	flushTimeout time.Duration
}

// EventRow represents a row in event_log.events.
// NOTE vs docs §2.1: docs use "timestamp_us BIGINT" — code uses "timestamp" (time.Time).
// Postgres stores time.Time as TIMESTAMPTZ which preserves microsecond precision.
// This is an intentional choice for better Postgres query ergonomics (range queries, etc).
type EventRow struct {
	Sequence       int64
	EventType      string
	IdempotencyKey string
	MarketID       *string
	Payload        []byte // JSON-encoded event payload
	StateHash      []byte
	PrevHash       []byte
	Timestamp      time.Time
	SourceSequence int64
}

// JournalRow represents a row in event_log.journal.
// NOTE vs docs §2.3: docs use "event_sequence" — code uses "sequence" (same semantics).
// Docs use "asset" (TEXT) — code uses "asset_id" (uint16, more efficient for joins).
// Docs use "timestamp_us" — code uses "timestamp" (int64 microseconds, same semantics).
// Code adds "event_ref" and "batch_id" not in docs (for traceability).
type JournalRow struct {
	JournalID     string
	BatchID       string
	EventRef      string
	Sequence      int64
	DebitAccount  string
	CreditAccount string
	AssetID       uint16
	Amount        int64
	JournalType   int32
	Timestamp     int64
}

func NewEventLogWriter(db *sql.DB, batchSize int, flushTimeout time.Duration) *EventLogWriter {
	return &EventLogWriter{
		db:           db,
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
	}
}

// WriteEventBatch writes a batch of events to event_log.events using COPY protocol.
// Per flow persistence-worker-batching-flowchart: uses Postgres COPY for high throughput.
// Pass a *sql.Tx to run inside a transaction, or nil to use the writer's default *sql.DB.
func (w *EventLogWriter) WriteEventBatch(ctx context.Context, events []EventRow, exec ...Executor) error {
	if len(events) == 0 {
		return nil
	}

	var e Executor = w.db
	if len(exec) > 0 && exec[0] != nil {
		e = exec[0]
	}

	stmt, err := e.(interface {
		Prepare(query string) (*sql.Stmt, error)
	}).Prepare(pq.CopyIn("event_log.events",
		"sequence", "event_type", "idempotency_key", "market_id",
		"payload", "state_hash", "prev_hash", "timestamp", "source_sequence",
	))
	if err != nil {
		// Fallback to multi-row INSERT if COPY not supported (e.g., in tests)
		return w.writeEventBatchInsert(ctx, events, e)
	}
	defer stmt.Close()

	for _, ev := range events {
		_, err := stmt.Exec(
			ev.Sequence, ev.EventType, ev.IdempotencyKey, ev.MarketID,
			ev.Payload, ev.StateHash, ev.PrevHash, ev.Timestamp, ev.SourceSequence,
		)
		if err != nil {
			return fmt.Errorf("COPY event row seq=%d: %w", ev.Sequence, err)
		}
	}

	// Flush COPY buffer
	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("COPY events flush: %w", err)
	}

	return nil
}

// writeEventBatchInsert is the fallback multi-row INSERT for environments
// where COPY protocol is not available (e.g., certain test harnesses).
func (w *EventLogWriter) writeEventBatchInsert(ctx context.Context, events []EventRow, e Executor) error {
	for _, ev := range events {
		_, err := e.ExecContext(ctx,
			`INSERT INTO event_log.events
				(sequence, event_type, idempotency_key, market_id, payload, state_hash, prev_hash, timestamp, source_sequence)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (sequence) DO NOTHING`,
			ev.Sequence, ev.EventType, ev.IdempotencyKey, ev.MarketID,
			ev.Payload, ev.StateHash, ev.PrevHash, ev.Timestamp, ev.SourceSequence,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteJournalBatch writes a batch of journal entries to event_log.journal using COPY protocol.
// Per flow persistence-worker-batching-flowchart: uses Postgres COPY for high throughput.
// Pass a *sql.Tx to run inside a transaction, or nil to use the writer's default *sql.DB.
func (w *EventLogWriter) WriteJournalBatch(ctx context.Context, journals []JournalRow, exec ...Executor) error {
	if len(journals) == 0 {
		return nil
	}

	var e Executor = w.db
	if len(exec) > 0 && exec[0] != nil {
		e = exec[0]
	}

	stmt, err := e.(interface {
		Prepare(query string) (*sql.Stmt, error)
	}).Prepare(pq.CopyIn("event_log.journal",
		"journal_id", "batch_id", "event_ref", "sequence",
		"debit_account", "credit_account", "asset_id", "amount",
		"journal_type", "timestamp",
	))
	if err != nil {
		// Fallback to row-by-row INSERT if COPY not supported
		return w.writeJournalBatchInsert(ctx, journals, e)
	}
	defer stmt.Close()

	for _, j := range journals {
		_, err := stmt.Exec(
			j.JournalID, j.BatchID, j.EventRef, j.Sequence,
			j.DebitAccount, j.CreditAccount, j.AssetID, j.Amount,
			j.JournalType, j.Timestamp,
		)
		if err != nil {
			return fmt.Errorf("COPY journal row %s: %w", j.JournalID, err)
		}
	}

	// Flush COPY buffer
	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("COPY journals flush: %w", err)
	}

	return nil
}

// writeJournalBatchInsert is the fallback row-by-row INSERT for environments
// where COPY protocol is not available.
func (w *EventLogWriter) writeJournalBatchInsert(ctx context.Context, journals []JournalRow, e Executor) error {
	for _, j := range journals {
		_, err := e.ExecContext(ctx,
			`INSERT INTO event_log.journal
				(journal_id, batch_id, event_ref, sequence, debit_account, credit_account, asset_id, amount, journal_type, timestamp)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (journal_id) DO NOTHING`,
			j.JournalID, j.BatchID, j.EventRef, j.Sequence,
			j.DebitAccount, j.CreditAccount, j.AssetID, j.Amount,
			j.JournalType, j.Timestamp,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateSchema is deprecated — use Migrator.Up() with migrations/*.sql instead.
// Kept as a no-op for backward compatibility during transition.
func (w *EventLogWriter) CreateSchema(ctx context.Context) error {
	return nil
}

// MarshalEventPayload serializes an event payload to JSON for storage.
func MarshalEventPayload(payload interface{}) ([]byte, error) {
	return json.Marshal(payload)
}
