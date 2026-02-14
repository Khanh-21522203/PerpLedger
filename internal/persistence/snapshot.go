package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SnapshotManager handles creating and loading state snapshots for recovery.
// Per doc §11: snapshots contain balances, positions, mark prices, funding state,
// idempotency LRU, sequence counters, and the last state hash.
type SnapshotManager struct {
	db *sql.DB
}

// SnapshotData contains the full in-memory state at a point in time.
type SnapshotData struct {
	Sequence          int64                      `json:"sequence"`
	StateHash         []byte                     `json:"state_hash"`
	PrevHash          []byte                     `json:"prev_hash"`
	Balances          map[string]int64           `json:"balances"`           // AccountPath -> balance
	Positions         []PositionSnapshot         `json:"positions"`
	MarkPrices        map[string]MarkPriceSnap   `json:"mark_prices"`       // marketID -> price state
	FundingSnapshots  map[string]FundingSnap     `json:"funding_snapshots"`  // "market:epoch" -> snapshot
	FundingNextEpochs map[string]int64           `json:"funding_next_epochs"` // marketID -> next epoch
	SequenceState     map[string]int64           `json:"sequence_state"`     // partition -> next expected seq
	IdempotencyKeys   []string                   `json:"idempotency_keys"`   // Recent keys for LRU warming
	CreatedAt         time.Time                  `json:"created_at"`
}

// PositionSnapshot is a serializable position.
type PositionSnapshot struct {
	UserID           string `json:"user_id"`
	MarketID         string `json:"market_id"`
	Side             int32  `json:"side"`
	Size             int64  `json:"size"`
	AvgEntryPrice    int64  `json:"avg_entry_price"`
	RealizedPnL      int64  `json:"realized_pnl"`
	LastFundingEpoch int64  `json:"last_funding_epoch"`
	LiquidationState int32  `json:"liquidation_state"`
	Version          int64  `json:"version"`
}

// MarkPriceSnap is a serializable mark price state.
type MarkPriceSnap struct {
	Price         int64 `json:"price"`
	PriceSequence int64 `json:"price_sequence"`
	Timestamp     int64 `json:"timestamp"`
}

// FundingSnap is a serializable funding snapshot.
type FundingSnap struct {
	MarketID    string `json:"market_id"`
	EpochID     int64  `json:"epoch_id"`
	FundingRate int64  `json:"funding_rate"`
	MarkPrice   int64  `json:"mark_price"`
	Timestamp   int64  `json:"timestamp"`
}

func NewSnapshotManager(db *sql.DB) *SnapshotManager {
	return &SnapshotManager{db: db}
}

// CreateSnapshotTable is deprecated — use Migrator.Up() with migrations/*.sql instead.
// Kept as a no-op for backward compatibility during transition.
func (sm *SnapshotManager) CreateSnapshotTable(ctx context.Context) error {
	return nil
}

// SaveSnapshot persists a snapshot to Postgres.
// Per doc §11: snapshots are taken periodically (e.g., every 100k events)
// and verified by replaying events from the snapshot sequence forward.
func (sm *SnapshotManager) SaveSnapshot(ctx context.Context, snap *SnapshotData) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	snapshotID := uuid.New()
	sizeBytes := len(data)
	formatVersion := int32(1) // v1: JSON-encoded SnapshotData

	_, err = sm.db.ExecContext(ctx, `
		INSERT INTO event_log.snapshots 
			(snapshot_id, sequence, data, state_hash, format_version, size_bytes, verified, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7)
		ON CONFLICT (sequence) DO UPDATE SET data = $3, state_hash = $4, size_bytes = $6
	`, snapshotID, snap.Sequence, data, snap.StateHash, formatVersion, sizeBytes, snap.CreatedAt)

	return err
}

// LoadLatestSnapshot loads the most recent verified snapshot.
// Per doc §11: on warm restart, load latest snapshot then replay events from snapshot.sequence+1.
func (sm *SnapshotManager) LoadLatestSnapshot(ctx context.Context) (*SnapshotData, error) {
	row := sm.db.QueryRowContext(ctx, `
		SELECT data FROM event_log.snapshots
		WHERE verified = TRUE
		ORDER BY sequence DESC
		LIMIT 1
	`)

	var data []byte
	if err := row.Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No snapshot — cold start
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	var snap SnapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}

	return &snap, nil
}

// MarkVerified marks a snapshot as verified after integrity check.
func (sm *SnapshotManager) MarkVerified(ctx context.Context, sequence int64) error {
	_, err := sm.db.ExecContext(ctx, `
		UPDATE event_log.snapshots SET verified = TRUE WHERE sequence = $1
	`, sequence)
	return err
}

// LoadEventsFrom loads events from a given sequence for replay.
// Per doc §11: used for warm restart (replay from snapshot) and cold restart (replay all).
func (sm *SnapshotManager) LoadEventsFrom(ctx context.Context, fromSequence int64, limit int) ([]EventRow, error) {
	rows, err := sm.db.QueryContext(ctx, `
		SELECT sequence, event_type, idempotency_key, market_id, payload, 
		       state_hash, prev_hash, timestamp, source_sequence
		FROM event_log.events
		WHERE sequence >= $1
		ORDER BY sequence ASC
		LIMIT $2
	`, fromSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []EventRow
	for rows.Next() {
		var e EventRow
		if err := rows.Scan(
			&e.Sequence, &e.EventType, &e.IdempotencyKey, &e.MarketID,
			&e.Payload, &e.StateHash, &e.PrevHash, &e.Timestamp, &e.SourceSequence,
		); err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, rows.Err()
}

// GetLatestSequence returns the highest sequence in the event log.
func (sm *SnapshotManager) GetLatestSequence(ctx context.Context) (int64, error) {
	var seq sql.NullInt64
	err := sm.db.QueryRowContext(ctx, `
		SELECT MAX(sequence) FROM event_log.events
	`).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil // Empty event log
	}
	return seq.Int64, nil
}
