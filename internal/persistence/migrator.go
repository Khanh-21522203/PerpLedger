package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Migrator runs SQL migration files in order.
// Compatible with golang-migrate file naming: {version}_{name}.up.sql / .down.sql
type Migrator struct {
	db            *sql.DB
	migrationsDir string
}

func NewMigrator(db *sql.DB, migrationsDir string) *Migrator {
	return &Migrator{db: db, migrationsDir: migrationsDir}
}

// Up applies all pending up-migrations in order.
func (m *Migrator) Up(ctx context.Context) error {
	if err := m.ensureMigrationTable(ctx); err != nil {
		return fmt.Errorf("ensure migration table: %w", err)
	}

	applied, err := m.getAppliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("get applied versions: %w", err)
	}

	files, err := m.listMigrationFiles(".up.sql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}

	for _, f := range files {
		version := extractVersion(f)
		if applied[version] {
			continue
		}

		log.Printf("INFO: applying migration %s", f)
		content, err := os.ReadFile(filepath.Join(m.migrationsDir, f))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", f, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("exec migration %s: %w", f, err)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO public.schema_migrations (version, filename) VALUES ($1, $2)`,
			version, f,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %s: %w", f, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", f, err)
		}

		log.Printf("INFO: applied migration %s", f)
	}

	return nil
}

// Down rolls back the last applied migration.
func (m *Migrator) Down(ctx context.Context) error {
	if err := m.ensureMigrationTable(ctx); err != nil {
		return err
	}

	// Get the latest applied version
	var version, filename string
	err := m.db.QueryRowContext(ctx,
		`SELECT version, filename FROM public.schema_migrations ORDER BY version DESC LIMIT 1`,
	).Scan(&version, &filename)
	if err == sql.ErrNoRows {
		log.Println("INFO: no migrations to roll back")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get latest migration: %w", err)
	}

	// Find corresponding down file
	downFile := strings.Replace(filename, ".up.sql", ".down.sql", 1)
	content, err := os.ReadFile(filepath.Join(m.migrationsDir, downFile))
	if err != nil {
		return fmt.Errorf("read down migration %s: %w", downFile, err)
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, string(content)); err != nil {
		tx.Rollback()
		return fmt.Errorf("exec down migration %s: %w", downFile, err)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM public.schema_migrations WHERE version = $1`, version,
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("remove migration record %s: %w", version, err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("INFO: rolled back migration %s", downFile)
	return nil
}

func (m *Migrator) ensureMigrationTable(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS public.schema_migrations (
			version    TEXT PRIMARY KEY,
			filename   TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	return err
}

func (m *Migrator) getAppliedVersions(ctx context.Context) (map[string]bool, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT version FROM public.schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func (m *Migrator) listMigrationFiles(suffix string) ([]string, error) {
	entries, err := os.ReadDir(m.migrationsDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			files = append(files, e.Name())
		}
	}

	sort.Strings(files)
	return files, nil
}

// extractVersion returns the numeric prefix from a migration filename.
// e.g. "000001_event_log.up.sql" → "000001"
func extractVersion(filename string) string {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return filename
}
