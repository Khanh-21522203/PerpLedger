package observability

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// NewLogger creates a structured JSON logger per doc §18 §3.
// Log format: structured JSON to stdout.
// Production default: info. Set via PERP_LOG_LEVEL env var.
func NewLogger(component string) zerolog.Logger {
	level := parseLogLevel(os.Getenv("PERP_LOG_LEVEL"))

	return zerolog.New(os.Stdout).
		Level(level).
		With().
		Timestamp().
		Str("component", component).
		Logger()
}

// NewLoggerWithLevel creates a logger with an explicit level.
func NewLoggerWithLevel(component string, level zerolog.Level) zerolog.Logger {
	return zerolog.New(os.Stdout).
		Level(level).
		With().
		Timestamp().
		Str("component", component).
		Logger()
}

func parseLogLevel(s string) zerolog.Level {
	switch s {
	case "debug":
		return zerolog.DebugLevel
	case "info", "":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}

func init() {
	// Per doc §18 §3.1: timestamps in RFC3339 with microsecond precision
	zerolog.TimeFieldFormat = time.RFC3339Nano
}
