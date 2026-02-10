package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// EventLogWriter writes events and journals to Postgres using batch inserts.
// Per doc §11: the persistence worker uses COPY protocol for high throughput.
// This implementation uses multi-row INSERT as a portable alternative;
// switch to pgx CopyFrom for production-grade throughput.
type EventLogWriter struct {
	db           *sql.DB
	batchSize    int
	flushTimeout time.Duration
}

// EventRow represents a row in event_log.events
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

// JournalRow represents a row in event_log.journal
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

// WriteEventBatch writes a batch of events to event_log.events using multi-row INSERT.
func (w *EventLogWriter) WriteEventBatch(ctx context.Context, events []EventRow) error {
	if len(events) == 0 {
		return nil
	}

	// Build multi-row INSERT
	query := `INSERT INTO event_log.events 
		(sequence, event_type, idempotency_key, market_id, payload, state_hash, prev_hash, timestamp, source_sequence)
		VALUES `

	values := make([]string, 0, len(events))
	args := make([]interface{}, 0, len(events)*9)

	for i, e := range events {
		base := i * 9
		values = append(values, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
		))
		args = append(args,
			e.Sequence, e.EventType, e.IdempotencyKey, e.MarketID,
			e.Payload, e.StateHash, e.PrevHash, e.Timestamp, e.SourceSequence,
		)
	}

	query += strings.Join(values, ", ")
	query += " ON CONFLICT (sequence) DO NOTHING" // Idempotent writes

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

// WriteJournalBatch writes a batch of journal entries to event_log.journal.
func (w *EventLogWriter) WriteJournalBatch(ctx context.Context, journals []JournalRow) error {
	if len(journals) == 0 {
		return nil
	}

	query := `INSERT INTO event_log.journal
		(journal_id, batch_id, event_ref, sequence, debit_account, credit_account, asset_id, amount, journal_type, timestamp)
		VALUES `

	values := make([]string, 0, len(journals))
	args := make([]interface{}, 0, len(journals)*10)

	for i, j := range journals {
		base := i * 10
		values = append(values, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
		))
		args = append(args,
			j.JournalID, j.BatchID, j.EventRef, j.Sequence,
			j.DebitAccount, j.CreditAccount, j.AssetID, j.Amount,
			j.JournalType, j.Timestamp,
		)
	}

	query += strings.Join(values, ", ")
	query += " ON CONFLICT (journal_id) DO NOTHING"

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
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
