# PerpLedger

A **deterministic, event-sourced double-entry accounting ledger** for perpetual futures exchanges. Built in Go, PerpLedger processes all financial state transitions — deposits, withdrawals, trades, funding payments, margin, and liquidations — through a single-threaded deterministic core that guarantees reproducible state from any event log replay.

## Features

- **Double-Entry Ledger** — Every balance mutation is a balanced journal entry (`Σ debits == Σ credits`). The system is provably zero-sum at all times.
- **Deterministic Core** — Single-threaded event processor with no wall-clock dependencies. Replay the same events → get the same state, byte-for-byte.
- **Event Sourcing** — All state is derived from an append-only event log. Supports cold restart (replay all) and warm restart (snapshot + replay delta).
- **State Hash Chain** — Each event produces a SHA-256 state hash chained to the previous, enabling cryptographic audit trails.
- **Idempotency** — Two-tier deduplication: in-memory LRU cache (1M keys) + Postgres fallback. Safe for at-least-once delivery.
- **Margin & Liquidation** — Real-time margin health checks on every mark price update, with automatic liquidation triggering when maintenance margin is breached.
- **Funding Payments** — Epoch-based funding rate snapshots and settlements with per-user journal generation and rounding residual handling.
- **Insurance Fund** — System account that absorbs liquidation deficits and funding shortfalls.
- **Projections** — Async projection workers update read-optimized tables (balances, positions, funding history, liquidation history) from the event stream.
- **Observability** — Prometheus metrics, structured logging (zerolog), health/readiness endpoints, Grafana dashboards.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ingestion Layer                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │NATS JetStream│    │ gRPC Ingest  │    │ gRPC Admin       │   │
│  │ (high-thru)  │    │ (manual)     │    │ (snapshots, etc) │   │
│  └──────┬───────┘    └──────┬───────┘    └──────────────────┘   │
│         │ RawEvent          │ event.Event                       │
│         ▼                   ▼                                   │
│  ┌─────────────────────────────────────┐                        │
│  │     Async Ingestion Shell           │                        │
│  │  (JSON parse, validate, sequence)   │                        │
│  └──────────────┬──────────────────────┘                        │
└─────────────────┼───────────────────────────────────────────────┘
                  │ event.Event
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              Deterministic Core (single-threaded)               │
│                                                                 │
│  ┌────────────┐ ┌──────────────┐ ┌───────────────────────────┐  │
│  │ Idempotency│ │  Sequence    │ │    Event Dispatch         │  │
│  │ Checker    │ │  Validator   │ │  (deposit/trade/funding/…)│  │
│  └────────────┘ └──────────────┘ └─────────────┬─────────────┘  │
│                                                │                │
│  ┌──────────────┐ ┌──────────────┐ ┌───────────▼─────────────┐  │
│  │ Balance      │ │ Position     │ │ Journal Generator       │  │
│  │ Tracker      │ │ Manager      │ │ (double-entry batches)  │  │
│  └──────────────┘ └──────────────┘ └─────────────────────────┘  │
│  ┌──────────────┐ ┌──────────────┐ ┌─────────────────────────┐  │
│  │ Margin       │ │ Funding      │ │ State Hasher            │  │
│  │ Calculator   │ │ Manager      │ │ (SHA-256 chain)         │  │
│  └──────────────┘ └──────────────┘ └─────────────────────────┘  │
└────────────┬────────────────────────────────┬───────────────────┘
             │ CoreOutput (blocking)          │ CoreOutput (non-blocking)
             ▼                                ▼
┌────────────────────────┐     ┌──────────────────────────────┐
│   Persistence Worker   │     │   Projection Worker          │
│  (batch INSERT, tx)    │     │  (balances, positions,       │
│  event_log.events      │     │   funding_history, etc.)     │
│  event_log.journal     │     │  projections.*               │
└────────────────────────┘     └──────────────────────────────┘
```

### Key Design Principles

1. **No locks in the core** — The deterministic core is single-threaded. No mutexes, no goroutines. All concurrency is at the shell boundary.
2. **Versioned inputs only** — The core never calls `time.Now()`. All timestamps are provided by upstream systems as versioned inputs.
3. **Blocking persistence, non-blocking projections** — The persist channel applies backpressure (no event loss). The projection channel drops silently (projections can be rebuilt).
4. **Atomic writes** — Events and journals are written in a single Postgres transaction per batch.

## Project Structure

```
PerpLedger/
├── cmd/
│   ├── perpledger/       # Main application entry point
│   └── migrate/          # Database migration CLI
├── internal/
│   ├── core/             # Deterministic event processor
│   ├── event/            # Event type definitions (13 event types)
│   ├── ingestion/        # NATS subscriber, JSON parser, gRPC ingest
│   ├── ledger/           # Account keys, balance tracker, journal generator, validator
│   ├── math/             # Fixed-point arithmetic, funding calculations
│   ├── observability/    # Metrics, logging, health checks
│   ├── persistence/      # Postgres writer, snapshot manager, idempotency DB
│   ├── projection/       # Async projection worker (balances, positions, etc.)
│   ├── query/            # Read-path query service
│   ├── server/           # gRPC + gRPC-Gateway HTTP server
│   └── state/            # Position, margin, funding, liquidation, risk params
├── migrations/           # Postgres schema migrations
├── proto/                # Protobuf definitions
├── gen/                  # Generated protobuf Go code
├── deploy/               # Docker, Prometheus, Grafana configs
├── docker-compose.yml    # Local dev: Postgres, NATS, Prometheus, Grafana
└── Makefile              # Build, test, migrate, docker commands
```

## Event Types

| Event | Source | Description |
|-------|--------|-------------|
| `DepositInitiated` | Custody | Pending deposit → locks in pending_deposit |
| `DepositConfirmed` | Custody | Confirmed deposit → credits user collateral |
| `WithdrawalRequested` | User | Locks collateral → pending_withdrawal |
| `WithdrawalConfirmed` | Custody | Finalizes withdrawal to external |
| `WithdrawalRejected` | Custody | Reverses pending → restores collateral |
| `TradeFill` | Matching Engine | Fee + margin reserve/release + realized PnL |
| `MarkPriceUpdate` | Oracle | Updates mark price, triggers margin checks |
| `FundingRateSnapshot` | Funding Engine | Stores epoch funding rate for settlement |
| `FundingEpochSettle` | Scheduler | Settles funding for all positions in market |
| `RiskParamUpdate` | Admin | Updates margin fractions, triggers margin recheck |
| `LiquidationTriggered` | Core (outbound) | Emitted when margin breaches MM |
| `LiquidationFill` | Liquidation Engine | Processes liquidation fill (closes position) |
| `LiquidationCompleted` | Liquidation Engine | Finalizes liquidation, insurance fund coverage |

## Getting Started

### Prerequisites

- **Go 1.21+**
- **Docker & Docker Compose** (for Postgres, NATS, Prometheus, Grafana)

### Quick Start

```bash
# 1. Start infrastructure (Postgres, NATS, Prometheus, Grafana)
make dev

# 2. Run the application
source .env.dev
go run ./cmd/perpledger
```

### Manual Setup

```bash
# Start dependencies
docker compose up -d

# Wait for services, then run migrations
go run ./cmd/migrate up

# Setup NATS JetStream streams
bash deploy/scripts/setup-nats.sh

# Run the ledger
source .env.dev
go run ./cmd/perpledger
```

### Build

```bash
make build          # → bin/perpledger
```

### Run Tests

```bash
make test           # All tests with race detector
make test-unit      # Unit tests only
make test-coverage  # Generate coverage report → coverage.html
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PERP_POSTGRES_DSN` | `postgres://perp:perp_dev_password@localhost:5432/perpledger?sslmode=disable` | Postgres connection string |
| `PERP_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `PERP_GRPC_ADDR` | `:9090` | gRPC server listen address |
| `PERP_HTTP_ADDR` | `:8080` | HTTP/JSON gateway listen address |
| `PERP_METRICS_ADDR` | `:9091` | Prometheus metrics endpoint |
| `PERP_PERSIST_CHAN_SIZE` | `1024` | Persistence channel buffer size |
| `PERP_PROJECTION_CHAN_SIZE` | `2048` | Projection channel buffer size |
| `PERP_PERSIST_BATCH_SIZE` | `50` | Events per persistence batch |
| `PERP_SNAPSHOT_INTERVAL` | `100000` | Take snapshot every N events |
| `PERP_IDEMPOTENCY_LRU_CAPACITY` | `1000000` | In-memory idempotency cache size |
| `PERP_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `GOGC` | `400` | Go GC target percentage (reduced GC pressure) |

## API

### gRPC Services

- **IngestService** (`:9090`) — `SubmitEvent`, `UpdateRiskParams`, `BackfillEvent`
- **QueryService** (`:9090`) — `GetBalance`, `GetPosition`, `GetJournalEntries`, `GetEventLog`
- **AdminService** (`:9090`) — `TakeSnapshot`, `RebuildProjections`, `GetSystemStatus`

### HTTP/JSON Gateway

All gRPC endpoints are also available via REST at `:8080` through grpc-gateway:

```bash
# Get user balance
curl http://localhost:8080/v1/query/balance/{user_id}/{asset}

# Get positions
curl http://localhost:8080/v1/query/positions/{user_id}

# Submit event
curl -X POST http://localhost:8080/v1/ingest/event \
  -H "Content-Type: application/json" \
  -d '{"envelope": {"event_type": 3, "payload": "..."}}'
```

### Prometheus Metrics (`:9091/metrics`)

- `perpledger_core_events_applied_total` — Events processed by type
- `perpledger_core_event_duration_seconds` — Processing latency histogram
- `perpledger_core_sequence` — Current event sequence (gauge)
- `perpledger_persist_batch_duration_seconds` — Persistence batch write latency
- `perpledger_persist_errors_total` — Persistence error counter
- `perpledger_projection_update_duration_seconds` — Projection update latency

## Database Schema

### Event Log (`event_log` schema)

- **`events`** — Append-only event log with sequence, type, payload, state hash chain
- **`journal`** — Double-entry journal entries linked to events
- **`snapshots`** — Periodic state snapshots for fast recovery

### Projections (`projections` schema)

- **`balances`** — Materialized account balances (account_path, asset_id, balance)
- **`positions`** — Current position state per user per market
- **`funding_history`** — Historical funding payments
- **`liquidation_history`** — Liquidation lifecycle tracking
- **`watermark`** — Projection worker progress tracking

### Migrations

```bash
make migrate-up     # Apply all pending migrations
make migrate-down   # Rollback last migration
```

## Observability

| Service | URL | Description |
|---------|-----|-------------|
| Prometheus | `http://localhost:9092` | Metrics collection |
| Grafana | `http://localhost:3000` | Dashboards (admin/admin) |
| NATS Monitor | `http://localhost:8222` | JetStream monitoring |
| Health | `http://localhost:8080/healthz` | Liveness probe |
| Ready | `http://localhost:8080/readyz` | Readiness probe |

## License

Private — All rights reserved.
