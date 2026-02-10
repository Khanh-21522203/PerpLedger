package persistence

import (
	"PerpLedger/internal/observability"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"
)

// CoreOutput mirrors core.CoreOutput to avoid import cycle.
// The orchestrator (cmd/main.go) bridges between core.CoreOutput and this.
type CoreOutput struct {
	EventRow    EventRow
	JournalRows []JournalRow
}

// PersistenceWorker drains the persist channel and batch-writes to Postgres.
// Per doc §12: this goroutine runs independently from the deterministic core.
// The persist channel uses BLOCKING sends from the core, so if this worker
// falls behind, the core stalls — guaranteeing no event is lost.
type PersistenceWorker struct {
	writer       *EventLogWriter
	inputChan    <-chan CoreOutput
	batchSize    int
	flushTimeout time.Duration
	metrics      *observability.Metrics
}

func NewPersistenceWorker(
	db *sql.DB,
	inputChan <-chan CoreOutput,
	batchSize int,
	flushTimeout time.Duration,
	metrics *observability.Metrics,
) *PersistenceWorker {
	return &PersistenceWorker{
		writer:       NewEventLogWriter(db, batchSize, flushTimeout),
		inputChan:    inputChan,
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
		metrics:      metrics,
	}
}

// Run starts the persistence worker loop. It batches incoming outputs
// and flushes either when the batch is full or the flush timeout expires.
// Blocks until ctx is cancelled.
func (pw *PersistenceWorker) Run(ctx context.Context) error {
	eventBatch := make([]EventRow, 0, pw.batchSize)
	journalBatch := make([]JournalRow, 0, pw.batchSize*4) // ~4 journals per event avg

	timer := time.NewTimer(pw.flushTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: flush remaining
			if len(eventBatch) > 0 {
				if err := pw.flush(ctx, eventBatch, journalBatch); err != nil {
					log.Printf("ERROR: final flush failed: %v", err)
				}
			}
			return ctx.Err()

		case output, ok := <-pw.inputChan:
			if !ok {
				// Channel closed — flush and exit
				if len(eventBatch) > 0 {
					if err := pw.flush(context.Background(), eventBatch, journalBatch); err != nil {
						log.Printf("ERROR: final flush failed: %v", err)
					}
				}
				return nil
			}

			eventBatch = append(eventBatch, output.EventRow)
			journalBatch = append(journalBatch, output.JournalRows...)

			// Flush if batch is full
			if len(eventBatch) >= pw.batchSize {
				if err := pw.flush(ctx, eventBatch, journalBatch); err != nil {
					log.Printf("ERROR: batch flush failed: %v", err)
					// Continue — the data is still in the core's in-memory state
					// and will be re-persisted on recovery
				}
				eventBatch = eventBatch[:0]
				journalBatch = journalBatch[:0]
				timer.Reset(pw.flushTimeout)
			}

		case <-timer.C:
			// Flush timeout — write whatever we have
			if len(eventBatch) > 0 {
				if err := pw.flush(ctx, eventBatch, journalBatch); err != nil {
					log.Printf("ERROR: timeout flush failed: %v", err)
				}
				eventBatch = eventBatch[:0]
				journalBatch = journalBatch[:0]
			}
			timer.Reset(pw.flushTimeout)
		}
	}
}

func (pw *PersistenceWorker) flush(ctx context.Context, events []EventRow, journals []JournalRow) error {
	start := time.Now()

	// Write events and journals in a single transaction
	tx, err := pw.writer.db.BeginTx(ctx, nil)
	if err != nil {
		if pw.metrics != nil {
			pw.metrics.PersistErrors.WithLabelValues("tx_begin").Inc()
		}
		return err
	}
	defer tx.Rollback()

	if err := pw.writer.WriteEventBatch(ctx, events); err != nil {
		if pw.metrics != nil {
			pw.metrics.PersistErrors.WithLabelValues("write_events").Inc()
		}
		return err
	}

	if err := pw.writer.WriteJournalBatch(ctx, journals); err != nil {
		if pw.metrics != nil {
			pw.metrics.PersistErrors.WithLabelValues("write_journals").Inc()
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		if pw.metrics != nil {
			pw.metrics.PersistErrors.WithLabelValues("tx_commit").Inc()
		}
		return err
	}

	// Record metrics on success
	if pw.metrics != nil {
		pw.metrics.PersistBatchDur.Observe(time.Since(start).Seconds())
		pw.metrics.PersistBatchSize.Observe(float64(len(events)))
		pw.metrics.PersistEventsWritten.Add(float64(len(events)))
		pw.metrics.PersistJournalsWritten.Add(float64(len(journals)))
		if len(events) > 0 {
			pw.metrics.PersistLastSequence.Set(float64(events[len(events)-1].Sequence))
		}
	}

	return nil
}

// GetWriter returns the underlying writer for schema creation etc.
func (pw *PersistenceWorker) GetWriter() *EventLogWriter {
	return pw.writer
}

// MarshalPayload is a convenience wrapper for JSON-encoding event payloads.
func MarshalPayload(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("WARN: failed to marshal payload: %v", err)
		return []byte("{}")
	}
	return data
}
