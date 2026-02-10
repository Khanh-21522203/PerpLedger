package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestPostgresDSN returns the Postgres DSN for integration tests.
// Per doc §17: uses docker-compose.test.yml Postgres on port 5433.
func TestPostgresDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://perp_test:perp_test_password@localhost:5433/perpledger_test?sslmode=disable"
}

// TestNATSURL returns the NATS URL for integration tests.
// Per doc §17: uses docker-compose.test.yml NATS on port 4223.
func TestNATSURL() string {
	if url := os.Getenv("TEST_NATS_URL"); url != "" {
		return url
	}
	return "nats://localhost:4223"
}

// SetupTestDB creates a test database connection and runs migrations.
// Returns the *sql.DB and a cleanup function.
func SetupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	dsn := TestPostgresDSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("test postgres not available: %v (start with: docker compose -f docker-compose.test.yml up -d)", err)
	}

	cleanup := func() {
		// Clean all tables
		tables := []string{
			"event_log.events",
			"event_log.journal",
			"event_log.snapshots",
			"projections.balances",
			"projections.positions",
			"projections.funding_history",
			"projections.liquidation_history",
			"projections.watermark",
			"projections.metadata",
		}
		for _, table := range tables {
			db.Exec(fmt.Sprintf("TRUNCATE %s CASCADE", table))
		}
		db.Close()
	}

	return db, cleanup
}

// RequireIntegration skips the test if not running integration tests.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("skipping integration test (set INTEGRATION_TEST=1 to run)")
	}
}

// GoldenFile reads a golden file from testdata/ and returns its contents.
// Per doc §17: golden file testing for determinism verification.
func GoldenFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file %s: %v", path, err)
	}
	return data
}

// UpdateGoldenFile writes data to a golden file.
// Only used when UPDATE_GOLDEN=1 is set.
func UpdateGoldenFile(t *testing.T, name string, data []byte) {
	t.Helper()
	if os.Getenv("UPDATE_GOLDEN") != "1" {
		return
	}
	path := filepath.Join("testdata", name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create testdata dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write golden file %s: %v", path, err)
	}
	t.Logf("updated golden file: %s", path)
}

// AssertGolden compares data against a golden file.
// If UPDATE_GOLDEN=1, updates the golden file instead.
func AssertGolden(t *testing.T, name string, got []byte) {
	t.Helper()

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		UpdateGoldenFile(t, name, got)
		return
	}

	want := GoldenFile(t, name)
	if string(got) != string(want) {
		t.Errorf("golden file mismatch for %s:\n--- want ---\n%s\n--- got ---\n%s",
			name, string(want), string(got))
	}
}
