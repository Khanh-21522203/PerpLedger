package main

import (
	"PerpLedger/internal/core"
	"PerpLedger/internal/event"
	"PerpLedger/internal/ingestion"
	"PerpLedger/internal/ledger"
	"PerpLedger/internal/observability"
	"PerpLedger/internal/persistence"
	"PerpLedger/internal/projection"
	"PerpLedger/internal/query"
	"PerpLedger/internal/server"
	"PerpLedger/internal/state"
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

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		PersistChanSize:        envIntOrDefault("PERP_PERSIST_CHAN_SIZE", 1024),    // Per doc §3: persist channel capacity
		ProjectionChanSize:     envIntOrDefault("PERP_PROJECTION_CHAN_SIZE", 2048), // Per doc §3: projection channel capacity
		PersistBatchSize:       envIntOrDefault("PERP_PERSIST_BATCH_SIZE", 50),     // Per doc §4.3: batch size
		PersistFlushTimeout:    10 * time.Millisecond,                              // Per doc §4.3: batch timeout
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
	// TODO: Using ObjectPool for frequent object creation, to reduce pressure on GC
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

	// --- Snapshot Restore ---
	// Per doc §11: restore in-memory state from snapshot
	if snap != nil {
		restoreStateFromSnapshot(deterministicCore, snap)
	}

	// --- LRU Warming ---
	// Per doc §10: warm LRU from snapshot to avoid cold-path DB lookups
	if snap != nil && len(snap.IdempotencyKeys) > 0 {
		log.Printf("INFO: warming LRU with %d keys from snapshot", len(snap.IdempotencyKeys))
		deterministicCore.WarmLRU(snap.IdempotencyKeys)
	}

	// --- Event Replay ---
	// Per doc §11: replay events from snapshot.sequence+1 to head
	replayCount, err := replayEventsFromLog(ctx, snapMgr, deterministicCore, startSequence)
	if err != nil {
		log.Fatalf("FATAL: event replay failed: %v", err)
	}
	if replayCount > 0 {
		log.Printf("INFO: replayed %d events (sequence now at %d)", replayCount, deterministicCore.GetSequence())
	}

	// --- State Hash Verification ---
	// Per doc §11: verify state hash after replay matches stored hash
	if snap != nil && replayCount == 0 {
		var expectedHash [32]byte
		copy(expectedHash[:], snap.StateHash)
		actualHash := deterministicCore.GetStateHash()
		if expectedHash != actualHash {
			log.Fatalf("FATAL: state hash mismatch after restore — expected %x, got %x", expectedHash, actualHash)
		}
		log.Println("INFO: state hash verified after snapshot restore")
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

	// 5b. gRPC → Core ingestion loop
	go func() {
		runGRPCIngestionLoop(ctx, eventChan, deterministicCore)
	}()

	// 6. gRPC server
	go func() {
		errChan <- grpcServer.StartGRPC(ctx)
	}()

	// 7. HTTP/JSON gateway (proxies to gRPC)
	go func() {
		errChan <- grpcServer.StartHTTPGateway(ctx)
	}()

	// 8. Periodic snapshot creation
	// Per doc §11: snapshots taken every N events for faster recovery
	go func() {
		runPeriodicSnapshots(ctx, deterministicCore, snapMgr, int(cfg.SnapshotInterval), metrics)
	}()

	// 9. Prometheus metrics server (per doc §18)
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
	// Per doc §3: drain channels, flush persistence, take final snapshot, then exit
	cancel()

	natsSubscriber.Stop()

	// Give workers time to flush
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	close(persistWorkerChan)
	close(projectionWorkerChan)
	close(publishChan)

	// Take final snapshot before exit (per doc graceful-shutdown-flowchart)
	if err := takeSnapshot(shutdownCtx, deterministicCore, snapMgr, metrics); err != nil {
		log.Printf("ERROR: final snapshot failed: %v", err)
	} else {
		log.Println("INFO: final snapshot saved")
	}

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
	// Build subject-prefix → event-type lookup from DefaultSubjects.
	// Subjects use ">" wildcard, so we match by prefix (strip trailing ".>").
	subjectToType := make(map[string]string)
	for _, cfg := range ingestion.DefaultSubjects() {
		prefix := cfg.Subject
		// Strip trailing ".>" for prefix matching
		if len(prefix) > 2 && prefix[len(prefix)-2:] == ".>" {
			prefix = prefix[:len(prefix)-2]
		}
		subjectToType[prefix] = cfg.EventType
	}

	// Per flow nats-ack-and-backpressure-sequence: messages are acked after
	// being sent to the InboundChannel (i.e. after parse+validate), NOT after
	// core processing. This prevents AckWait expiry during slow core processing
	// and naturally propagates backpressure via channel blocking.
	typedEventChan := make(chan event.Event, 4096)

	// Goroutine: parse raw events and forward to typed channel, then ack
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-rawChan:
				if !ok {
					close(typedEventChan)
					return
				}

				// Resolve event type from NATS subject by matching longest prefix
				eventType := resolveEventType(raw.Subject, subjectToType)
				if eventType == "" {
					log.Printf("WARN: unknown NATS subject: %s", raw.Subject)
					raw.AckFunc() // Ack invalid events to avoid redelivery loop
					continue
				}

				evt, err := ingestion.ParseRawEvent(raw, eventType)
				if err != nil {
					log.Printf("WARN: parse event failed (subject=%s): %v", raw.Subject, err)
					raw.AckFunc() // Ack unparseable events per flow (invalid events acked but not forwarded)
					continue
				}

				// Blocking send to typed channel — backpressure propagates to NATS
				select {
				case typedEventChan <- evt:
					raw.AckFunc() // Ack AFTER successful channel send
				case <-ctx.Done():
					raw.NakFunc()
					return
				}
			}
		}
	}()

	// Core processing loop: drain typed events
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-typedEventChan:
			if !ok {
				return
			}

			if err := core.ProcessEvent(evt); err != nil {
				log.Printf("ERROR: core.ProcessEvent failed (type=%s, key=%s): %v",
					evt.EventType(), evt.IdempotencyKey(), err)
				// Event already acked — core errors are logged but not retried via NATS.
				// The event is in the channel and will be persisted if it was a transient error,
				// or silently skipped if it was a validation error (dedup, gap, etc).
			}
		}
	}
}

// resolveEventType finds the event type for a NATS subject by matching the longest prefix.
func resolveEventType(subject string, prefixMap map[string]string) string {
	bestMatch := ""
	bestType := ""
	for prefix, evtType := range prefixMap {
		if len(subject) >= len(prefix) && subject[:len(prefix)] == prefix {
			if len(prefix) > len(bestMatch) {
				bestMatch = prefix
				bestType = evtType
			}
		}
	}
	return bestType
}

// runGRPCIngestionLoop reads typed events from the gRPC ingest channel and feeds them to the core.
// Per doc §15: gRPC ingest is for admin operations and manual event injection.
func runGRPCIngestionLoop(ctx context.Context, eventChan <-chan event.Event, core *core.DeterministicCore) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-eventChan:
			if !ok {
				return
			}

			if err := core.ProcessEvent(evt); err != nil {
				log.Printf("ERROR: core.ProcessEvent (gRPC) failed (type=%s, key=%s): %v",
					evt.EventType(), evt.IdempotencyKey(), err)
			}
		}
	}
}

// --- Snapshot Restore & Replay ---

// restoreStateFromSnapshot converts a persistence.SnapshotData into core.SnapshotState
// and restores the deterministic core's in-memory state.
func restoreStateFromSnapshot(deterministicCore *core.DeterministicCore, snap *persistence.SnapshotData) {
	coreSnap := &core.SnapshotState{
		Sequence:          snap.Sequence,
		Balances:          make(map[ledger.AccountKey]int64),
		MarkPrices:        make(map[string]*state.MarkPriceState),
		FundingSnapshots:  make(map[string]*state.FundingSnapshot),
		FundingNextEpochs: snap.FundingNextEpochs,
		SequenceState:     snap.SequenceState,
		IdempotencyKeys:   snap.IdempotencyKeys,
	}

	// Restore state hash
	copy(coreSnap.StateHash[:], snap.StateHash)
	copy(coreSnap.PrevHash[:], snap.PrevHash)

	// Convert balance map (string path → AccountKey)
	for path, balance := range snap.Balances {
		key := ledger.ParseAccountPath(path)
		coreSnap.Balances[key] = balance
	}

	// Convert positions
	for _, ps := range snap.Positions {
		userID, _ := uuid.Parse(ps.UserID)
		pos := &state.Position{
			UserID:           userID,
			MarketID:         ps.MarketID,
			Side:             event.Side(ps.Side),
			Size:             ps.Size,
			AvgEntryPrice:    ps.AvgEntryPrice,
			RealizedPnL:      ps.RealizedPnL,
			LastFundingEpoch: ps.LastFundingEpoch,
			LiquidationState: state.LiquidationState(ps.LiquidationState),
			Version:          ps.Version,
		}
		coreSnap.Positions = append(coreSnap.Positions, pos)
	}

	// Convert mark prices
	for marketID, mp := range snap.MarkPrices {
		coreSnap.MarkPrices[marketID] = &state.MarkPriceState{
			Price:         mp.Price,
			PriceSequence: mp.PriceSequence,
			Timestamp:     mp.Timestamp,
		}
	}

	// Convert funding snapshots
	for key, fs := range snap.FundingSnapshots {
		coreSnap.FundingSnapshots[key] = &state.FundingSnapshot{
			MarketID:    fs.MarketID,
			EpochID:     fs.EpochID,
			FundingRate: fs.FundingRate,
			MarkPrice:   fs.MarkPrice,
			Timestamp:   fs.Timestamp,
		}
	}

	deterministicCore.RestoreFromSnapshot(coreSnap)
	log.Printf("INFO: restored in-memory state from snapshot at sequence %d", snap.Sequence)
}

// replayEventsFromLog replays events from the event log starting at fromSequence.
// Per doc §11: used for warm restart (replay from snapshot) and cold restart (replay all).
func replayEventsFromLog(
	ctx context.Context,
	snapMgr *persistence.SnapshotManager,
	deterministicCore *core.DeterministicCore,
	fromSequence int64,
) (int64, error) {
	const batchSize = 1000
	var totalReplayed int64

	for {
		events, err := snapMgr.LoadEventsFrom(ctx, fromSequence, batchSize)
		if err != nil {
			return totalReplayed, fmt.Errorf("load events from seq %d: %w", fromSequence, err)
		}

		if len(events) == 0 {
			break
		}

		for _, evtRow := range events {
			// Parse the stored event payload back into a typed event
			raw := ingestion.RawEvent{
				Subject: evtRow.EventType,
				Data:    evtRow.Payload,
			}

			typedEvt, err := ingestion.ParseRawEvent(raw, evtRow.EventType)
			if err != nil {
				log.Printf("WARN: skip unparseable event at seq=%d type=%s: %v",
					evtRow.Sequence, evtRow.EventType, err)
				continue
			}

			if err := deterministicCore.ProcessEvent(typedEvt); err != nil {
				// During replay, duplicates and sequence errors are expected — skip
				log.Printf("DEBUG: replay skip seq=%d: %v", evtRow.Sequence, err)
			}

			totalReplayed++
		}

		fromSequence = events[len(events)-1].Sequence + 1
	}

	return totalReplayed, nil
}

// --- Snapshot Helpers ---

// runPeriodicSnapshots takes snapshots every N events.
// Per doc §11: snapshots are taken periodically for faster recovery.
func runPeriodicSnapshots(
	ctx context.Context,
	deterministicCore *core.DeterministicCore,
	snapMgr *persistence.SnapshotManager,
	interval int,
	metrics *observability.Metrics,
) {
	if interval <= 0 {
		interval = 100_000 // Default: every 100k events
	}

	lastSnapshotSeq := deterministicCore.GetSequence()
	ticker := time.NewTicker(10 * time.Second) // Check every 10s
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentSeq := deterministicCore.GetSequence()
			if currentSeq-lastSnapshotSeq >= int64(interval) {
				if err := takeSnapshot(ctx, deterministicCore, snapMgr, metrics); err != nil {
					log.Printf("WARN: periodic snapshot failed: %v", err)
				} else {
					lastSnapshotSeq = currentSeq
					log.Printf("INFO: periodic snapshot at sequence %d", currentSeq)
				}
			}
		}
	}
}

// takeSnapshot captures the core's in-memory state and persists it.
func takeSnapshot(
	ctx context.Context,
	deterministicCore *core.DeterministicCore,
	snapMgr *persistence.SnapshotManager,
	metrics *observability.Metrics,
) error {
	start := time.Now()

	coreSnap := deterministicCore.CreateSnapshotState()

	// Convert core.SnapshotState to persistence.SnapshotData
	snapData := &persistence.SnapshotData{
		Sequence:          coreSnap.Sequence,
		StateHash:         coreSnap.StateHash[:],
		Balances:          make(map[string]int64),
		Positions:         make([]persistence.PositionSnapshot, 0, len(coreSnap.Positions)),
		MarkPrices:        make(map[string]persistence.MarkPriceSnap),
		FundingSnapshots:  make(map[string]persistence.FundingSnap),
		FundingNextEpochs: coreSnap.FundingNextEpochs,
		SequenceState:     coreSnap.SequenceState,
		IdempotencyKeys:   coreSnap.IdempotencyKeys,
		CreatedAt:         time.Now(),
	}

	// Convert balances
	for key, balance := range coreSnap.Balances {
		snapData.Balances[key.AccountPath()] = balance
	}

	// Convert positions
	for _, pos := range coreSnap.Positions {
		snapData.Positions = append(snapData.Positions, persistence.PositionSnapshot{
			UserID:           pos.UserID.String(),
			MarketID:         pos.MarketID,
			Side:             int32(pos.Side),
			Size:             pos.Size,
			AvgEntryPrice:    pos.AvgEntryPrice,
			RealizedPnL:      pos.RealizedPnL,
			LastFundingEpoch: pos.LastFundingEpoch,
			LiquidationState: int32(pos.LiquidationState),
			Version:          pos.Version,
		})
	}

	// Convert mark prices
	for marketID, mp := range coreSnap.MarkPrices {
		snapData.MarkPrices[marketID] = persistence.MarkPriceSnap{
			Price:         mp.Price,
			PriceSequence: mp.PriceSequence,
			Timestamp:     mp.Timestamp,
		}
	}

	// Convert funding snapshots
	for key, fs := range coreSnap.FundingSnapshots {
		snapData.FundingSnapshots[key] = persistence.FundingSnap{
			MarketID:    fs.MarketID,
			EpochID:     fs.EpochID,
			FundingRate: fs.FundingRate,
			MarkPrice:   fs.MarkPrice,
			Timestamp:   fs.Timestamp,
		}
	}

	if err := snapMgr.SaveSnapshot(ctx, snapData); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	// Mark as verified immediately (we just created it from live state)
	if err := snapMgr.MarkVerified(ctx, snapData.Sequence); err != nil {
		log.Printf("WARN: mark snapshot verified failed: %v", err)
	}

	if metrics != nil {
		metrics.SnapshotTaken.Inc()
		metrics.SnapshotDuration.Observe(time.Since(start).Seconds())
		metrics.SnapshotLastSeq.Set(float64(snapData.Sequence))
	}

	return nil
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
