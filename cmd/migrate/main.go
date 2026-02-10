package main

import (
	"PerpLedger/internal/persistence"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: migrate <up|down>")
		fmt.Println("  up   - apply all pending migrations")
		fmt.Println("  down - roll back the last migration")
		fmt.Println()
		fmt.Println("Environment:")
		fmt.Println("  POSTGRES_URL    - Postgres connection string (required)")
		fmt.Println("  MIGRATIONS_DIR  - path to migrations directory (default: migrations)")
		os.Exit(1)
	}

	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = "postgres://localhost:5432/perpledger?sslmode=disable"
	}

	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "migrations"
	}

	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("FATAL: open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	migrator := persistence.NewMigrator(db, migrationsDir)

	switch os.Args[1] {
	case "up":
		if err := migrator.Up(ctx); err != nil {
			log.Fatalf("FATAL: migrate up: %v", err)
		}
		log.Println("INFO: all migrations applied")

	case "down":
		if err := migrator.Down(ctx); err != nil {
			log.Fatalf("FATAL: migrate down: %v", err)
		}
		log.Println("INFO: last migration rolled back")

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s (use 'up' or 'down')\n", os.Args[1])
		os.Exit(1)
	}
}
