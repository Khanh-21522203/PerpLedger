package persistence

import (
	"context"
	"database/sql"
	"time"
)

// PostgresIdempotencyChecker implements DB-based deduplication
type PostgresIdempotencyChecker struct {
	db *sql.DB
}

func NewPostgresIdempotencyChecker(db *sql.DB) *PostgresIdempotencyChecker {
	return &PostgresIdempotencyChecker{
		db: db,
	}
}

// IsDuplicate checks if event exists in Postgres event log
func (pic *PostgresIdempotencyChecker) IsDuplicate(eventType string, idempotencyKey string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	query := `
        SELECT 1 
        FROM event_log.events 
        WHERE event_type = $1 AND idempotency_key = $2 
        LIMIT 1
    `

	var exists int
	err := pic.db.QueryRowContext(ctx, query, eventType, idempotencyKey).Scan(&exists)

	if err == sql.ErrNoRows {
		return false, nil // Not found - not a duplicate
	}

	if err != nil {
		return false, err // DB error
	}

	return true, nil // Found - is duplicate
}

// CreateIdempotencyIndex creates the unique index for deduplication
func (pic *PostgresIdempotencyChecker) CreateIdempotencyIndex() error {
	_, err := pic.db.Exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idem 
        ON events (event_type, idempotency_key)
    `)
	return err
}
