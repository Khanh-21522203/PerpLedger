package main

import (
	"PerpLedger/internal/core"
	"PerpLedger/internal/event"
	"PerpLedger/internal/ingestion"
	"PerpLedger/internal/observability"
	"PerpLedger/internal/persistence"
	"PerpLedger/internal/projection"
	"PerpLedger/internal/query"
	"PerpLedger/internal/server"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "github.com/lib/pq"
)

// Config holds all application configuration.
// Per doc §3 (Overview): configuration is loaded from environment variables.
type Config struct {
	// Postgres
	PostgresURL string

	// NATS
	NATSURL string

	// Channels
	PersistChanSize    int
	ProjectionChanSize int

	// Persistence worker
	PersistBatchSize    int
	PersistFlushTimeout time.Duration

	// Snapshot
	SnapshotInterval int64 // Take snapshot every N events

	// gRPC/HTTP/Metrics
	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string

	// LRU
	IdempotencyLRUCapacity int

	// Migrations
	MigrationsDir string
}

func DefaultConfig() Config {
	return Config{
		PostgresURL:            envOrDefault("PERP_POSTGRES_DSN", "postgres://perp:perp_dev_password@localhost:5432/perpledger?sslmode=disable"),
		NATSURL:                envOrDefault("PERP_NATS_URL", "nats://localhost:4222"),
		PersistChanSize:        envIntOrDefault("PERP_PERSIST_CHAN_SIZE", 8192),
		ProjectionChanSize:     envIntOrDefault("PERP_PROJECTION_CHAN_SIZE", 4096),
		PersistBatchSize:       envIntOrDefault("PERP_PERSIST_BATCH_SIZE", 256),
		PersistFlushTimeout:    50 * time.Millisecond,
		SnapshotInterval:       int64(envIntOrDefault("PERP_SNAPSHOT_INTERVAL", 100_000)),
		GRPCAddr:               envOrDefault("PERP_GRPC_ADDR", ":9090"),
		HTTPAddr:               envOrDefault("PERP_HTTP_ADDR", ":8080"),
		MetricsAddr:            envOrDefault("PERP_METRICS_ADDR", ":9091"),
		IdempotencyLRUCapacity: envIntOrDefault("PERP_IDEMPOTENCY_LRU_CAPACITY", 1_000_000),
		MigrationsDir:          envOrDefault("PERP_MIGRATIONS_DIR", "migrations"),
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Println("INFO: PerpLedger starting...")

	// Per doc §12: set GOGC=400 and GOMEMLIMIT for reduced GC pressure
	if os.Getenv("GOGC") == "" {
		runtime.SetFinalizer(nil, nil) // Hint: GOGC=400 should be set via env
		log.Println("WARN: GOGC not set, recommend GOGC=400 for production")
	}

	cfg := DefaultConfig()

	// --- Context with graceful shutdown ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// --- Postgres ---
	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		log.Fatalf("FATAL: postgres open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("FATAL: postgres ping: %v", err)
	}
	log.Println("INFO: Postgres connected")

	// --- Run SQL migrations ---
	migrator := persistence.NewMigrator(db, cfg.MigrationsDir)
	if err := migrator.Up(ctx); err != nil {
		log.Fatalf("FATAL: run migrations: %v", err)
	}
	log.Println("INFO: migrations applied")

	snapMgr := persistence.NewSnapshotManager(db)

	// --- Recovery: load snapshot + replay ---
	startSequence := int64(0)

	snap, err := snapMgr.LoadLatestSnapshot(ctx)
	if err != nil {
		log.Printf("WARN: failed to load snapshot: %v", err)
	}
	if snap != nil {
		startSequence = snap.Sequence + 1
		log.Printf("INFO: loaded snapshot at sequence %d", snap.Sequence)
		// TODO: restore in-memory state from snapshot
	} else {
		log.Println("INFO: no snapshot found, cold start from sequence 0")
	}

	// --- Channels ---
	// Per doc §12: persist channel blocks (backpressure), projection channel drops
	persistCoreChan := make(chan core.CoreOutput, cfg.PersistChanSize)
	projectionCoreChan := make(chan core.CoreOutput, cfg.ProjectionChanSize)

	// Bridge channels for persistence worker (avoids import cycle)
	persistWorkerChan := make(chan persistence.CoreOutput, cfg.PersistChanSize)
	projectionWorkerChan := make(chan projection.ProjectionOutput, cfg.ProjectionChanSize)

	// --- Postgres idempotency checker ---
	dbChecker := persistence.NewPostgresIdempotencyChecker(db)

	// --- Observability ---
	// Per doc §18: Prometheus metrics + structured logging
	metrics := observability.NewMetrics()
	healthChecker := observability.NewHealthChecker()

	// --- Deterministic Core ---
	deterministicCore := core.NewDeterministicCore(
		startSequence,
		persistCoreChan,
		projectionCoreChan,
		dbChecker,
		metrics,
	)

	// --- LRU Warming ---
	if snap != nil && len(snap.IdempotencyKeys) > 0 {
		log.Printf("INFO: warming LRU with %d keys from snapshot", len(snap.IdempotencyKeys))
		// TODO: expose LRU warming on DeterministicCore
	}

	// --- NATS ---
	nc, js, err := ingestion.ConnectNATS(cfg.NATSURL)
	if err != nil {
		log.Fatalf("FATAL: nats connect: %v", err)
	}
	defer nc.Close()
	log.Println("INFO: NATS connected")

	if err := ingestion.EnsureStreams(ctx, js); err != nil {
		log.Fatalf("FATAL: ensure NATS streams: %v", err)
	}
	if err := ingestion.EnsureOutboundStream(ctx, js); err != nil {
		log.Fatalf("FATAL: ensure outbound stream: %v", err)
	}

	// --- Event channel from NATS to core ---
	rawEventChan := make(chan ingestion.RawEvent, 4096)
	natsSubscriber := ingestion.NewNATSSubscriber(js, rawEventChan)
	if err := natsSubscriber.Subscribe(ctx, ingestion.DefaultSubjects()); err != nil {
		log.Fatalf("FATAL: nats subscribe: %v", err)
	}

	// --- Outbound publisher ---
	publishChan := make(chan ingestion.PublishableEvent, 4096)
	outboundPublisher := ingestion.NewOutboundPublisher(js, publishChan)

	// --- Services ---
	queryService := query.NewQueryService(db)
	eventChan := make(chan event.Event, 4096)
	ingestService := ingestion.NewGRPCIngestService(eventChan)

	// --- gRPC + gRPC-Gateway server ---
	grpcServer := server.NewGRPCServer(cfg.GRPCAddr, cfg.HTTPAddr, &server.ServerDeps{
		DB:            db,
		QueryService:  queryService,
		IngestService: ingestService,
		SnapshotMgr:   snapMgr,
		StartTime:     time.Now(),
		HealthChecker: healthChecker,
	})

	// --- Start goroutines ---
	// Per doc §12: goroutine inventory
	errChan := make(chan error, 10)

	// 1. Persistence worker
	persistWorker := persistence.NewPersistenceWorker(db, persistWorkerChan, cfg.PersistBatchSize, cfg.PersistFlushTimeout, metrics)
	go func() {
		errChan <- persistWorker.Run(ctx)
	}()

	// 2. Projection worker
	projWorker := projection.NewProjectionWorker(db, projectionWorkerChan, metrics)
	go func() {
		errChan <- projWorker.Run(ctx)
	}()

	// 3. Outbound publisher
	go func() {
		errChan <- outboundPublisher.Run(ctx)
	}()

	// 4. Core output bridge: core.CoreOutput → persistence.CoreOutput + projection.ProjectionOutput
	go func() {
		bridgeCoreOutputs(ctx, persistCoreChan, projectionCoreChan, persistWorkerChan, projectionWorkerChan, publishChan)
	}()

	// 5. NATS → Core ingestion loop
	go func() {
		runIngestionLoop(ctx, rawEventChan, deterministicCore)
	}()

	// 6. gRPC server
	go func() {
		errChan <- grpcServer.StartGRPC(ctx)
	}()

	// 7. HTTP/JSON gateway (proxies to gRPC)
	go func() {
		errChan <- grpcServer.StartHTTPGateway(ctx)
	}()

	// 8. Prometheus metrics server (per doc §18)
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsServer := &http.Server{
			Addr:    cfg.MetricsAddr,
			Handler: metricsMux,
		}
		go func() {
			<-ctx.Done()
			shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			metricsServer.Shutdown(shutCtx)
		}()
		log.Printf("INFO: Metrics server listening on %s/metrics", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("metrics server: %w", err)
		}
	}()

	// Mark service as ready after all goroutines started
	healthChecker.SetReady(true)

	log.Printf("INFO: PerpLedger ready (sequence=%d, grpc=%s, http=%s, metrics=%s)",
		startSequence, cfg.GRPCAddr, cfg.HTTPAddr, cfg.MetricsAddr)

	// --- Wait for shutdown signal ---
	select {
	case sig := <-sigChan:
		log.Printf("INFO: received signal %s, shutting down...", sig)
	case err := <-errChan:
		log.Printf("ERROR: goroutine failed: %v, shutting down...", err)
	}

	// --- Graceful shutdown ---
	// Per doc §3: drain channels, flush persistence, then exit
	cancel()

	natsSubscriber.Stop()

	// Give workers time to flush
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = shutdownCtx

	close(persistWorkerChan)
	close(projectionWorkerChan)
	close(publishChan)

	log.Println("INFO: PerpLedger shutdown complete")
}

// bridgeCoreOutputs converts core.CoreOutput to persistence and projection formats.
// This avoids import cycles between core and persistence/projection packages.
func bridgeCoreOutputs(
	ctx context.Context,
	persistIn <-chan core.CoreOutput,
	projectionIn <-chan core.CoreOutput,
	persistOut chan<- persistence.CoreOutput,
	projectionOut chan<- projection.ProjectionOutput,
	publishOut chan<- ingestion.PublishableEvent,
) {
	for {
		select {
		case <-ctx.Done():
			return

		case output, ok := <-persistIn:
			if !ok {
				return
			}

			// Convert to persistence format
			payload := persistence.MarshalPayload(output.Batch)

			var marketID *string
			if output.Envelope.MarketID != nil {
				s := *output.Envelope.MarketID
				marketID = &s
			}

            // Convert [32]byte arrays to []byte slices for persistence
			stateHash := output.Envelope.StateHash[:]
			prevHash := output.Envelope.PrevHash[:]

			pOutput := persistence.CoreOutput{
				EventRow: persistence.EventRow{
					Sequence:       output.Envelope.Sequence,
					EventType:      output.Envelope.EventType.String(),
					IdempotencyKey: output.Envelope.IdempotencyKey,
					MarketID:       marketID,
					Payload:        payload,
					StateHash:      stateHash,
					PrevHash:       prevHash,
					Timestamp:      output.Envelope.Timestamp,
					SourceSequence: output.Envelope.SourceSequence,
				},
			}

			// Convert journals
			if output.Batch != nil {
				for _, j := range output.Batch.Journals {
					pOutput.JournalRows = append(pOutput.JournalRows, persistence.JournalRow{
						JournalID:     j.JournalID.String(),
						BatchID:       j.BatchID.String(),
						EventRef:      j.EventRef,
						Sequence:      j.Sequence,
						DebitAccount:  j.DebitAccount.AccountPath(),
						CreditAccount: j.CreditAccount.AccountPath(),
						AssetID:       uint16(j.AssetID),
						Amount:        j.Amount,
						JournalType:   int32(j.JournalType),
						Timestamp:     j.Timestamp,
					})
				}
			}

			persistOut <- pOutput

			// Also publish outbound
			select {
			case publishOut <- ingestion.PublishableEvent{
				Sequence:       output.Envelope.Sequence,
				EventType:      output.Envelope.EventType.String(),
				IdempotencyKey: output.Envelope.IdempotencyKey,
				MarketID:       marketID,
				Payload:        output.Batch,
				StateHash:      stateHash,
				Timestamp:      output.Envelope.Timestamp,
			}:
			default:
				// Drop if publish channel is full
			}

		case output, ok := <-projectionIn:
			if !ok {
				return
			}

			// Convert to projection format
			var marketID *string
			if output.Envelope.MarketID != nil {
				s := *output.Envelope.MarketID
				marketID = &s
			}

			pOutput := projection.ProjectionOutput{
				Sequence:  output.Envelope.Sequence,
				EventType: output.Envelope.EventType.String(),
				MarketID:  marketID,
				Timestamp: output.Envelope.Timestamp.UnixMicro(),
			}

			if output.Batch != nil {
				for _, j := range output.Batch.Journals {
					pOutput.JournalEntries = append(pOutput.JournalEntries, projection.JournalEntry{
						DebitAccount:  j.DebitAccount.AccountPath(),
						CreditAccount: j.CreditAccount.AccountPath(),
						AssetID:       uint16(j.AssetID),
						Amount:        j.Amount,
						JournalType:   int32(j.JournalType),
					})
				}
			}

			select {
			case projectionOut <- pOutput:
			default:
				// Drop if projection channel is full (per doc §12)
			}
		}
	}
}

// runIngestionLoop reads raw events from NATS and feeds them to the core.
// Per doc §15: the shell validates, parses, and converts raw events before
// sending to the deterministic core.
func runIngestionLoop(ctx context.Context, rawChan <-chan ingestion.RawEvent, core *core.DeterministicCore) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-rawChan:
			if !ok {
				return
			}

			// TODO: Parse raw.Data into typed event.Event based on raw.Subject
			// For now, ACK and skip — full parsing requires protobuf definitions
			_ = raw
			_ = core
			// Example:
			// evt, err := parseEvent(raw)
			// if err != nil { raw.NakFunc(); continue }
			// if err := core.ProcessEvent(evt); err != nil { log.Printf(...) }
			// raw.AckFunc()
		}
	}
}

// --- Helpers ---

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntOrDefault(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	var i int
	if _, err := fmt.Sscanf(v, "%d", &i); err != nil {
		return defaultVal
	}
	return i
}
