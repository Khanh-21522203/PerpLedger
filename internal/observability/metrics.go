package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for PerpLedger.
// Per doc §18: all metrics defined with type, labels, and description.
type Metrics struct {
	// --- Core Processing (§18 §2.1) ---
	CoreEventsApplied  *prometheus.CounterVec
	CoreEventsRejected *prometheus.CounterVec
	CoreEventDuration  *prometheus.HistogramVec
	CoreJournals       *prometheus.CounterVec
	CoreStateHashDur   prometheus.Histogram
	CoreSequence       prometheus.Gauge

	// --- Latency (§18 §2.2) ---
	IngestToApply      *prometheus.HistogramVec
	ApplyToPersist     prometheus.Histogram
	QueryFreshnessLag  *prometheus.HistogramVec
	NATSPullLatency    *prometheus.HistogramVec
	PersistBatchDur    prometheus.Histogram
	ProjectionUpdateDur *prometheus.HistogramVec

	// --- Channel & Backpressure (§18 §2.3) ---
	ChannelSize        *prometheus.GaugeVec
	ChannelCapacity    *prometheus.GaugeVec
	ChannelUtilization *prometheus.GaugeVec
	ProjectionDrops    *prometheus.CounterVec
	PublishDrops       prometheus.Counter
	PersistBackpressure prometheus.Counter

	// --- Idempotency & Ordering (§18 §2.4) ---
	IdempotencyDuplicates *prometheus.CounterVec
	DedupLRUSize          prometheus.Gauge
	DedupLRUEvictions     prometheus.Counter
	DedupTier2Duration    prometheus.Histogram
	EventSequenceGap      *prometheus.CounterVec
	EventOutOfOrder       *prometheus.CounterVec

	// --- Funding (§18 §2.5) ---
	FundingEpochSettled    *prometheus.CounterVec
	FundingEpochDuration   *prometheus.HistogramVec
	FundingPositionsSettled *prometheus.CounterVec
	FundingTotalPaid       *prometheus.CounterVec
	FundingTotalReceived   *prometheus.CounterVec
	FundingRoundingResidual *prometheus.GaugeVec
	FundingInsufficientBal *prometheus.CounterVec

	// --- Liquidation (§18 §2.6) ---
	LiquidationTriggered *prometheus.CounterVec
	LiquidationCompleted *prometheus.CounterVec
	LiquidationFills     *prometheus.CounterVec
	LiquidationDeficit   *prometheus.CounterVec
	InsuranceFundBalance prometheus.Gauge

	// --- Persistence (§18 §2.7) ---
	PersistEventsWritten  prometheus.Counter
	PersistJournalsWritten prometheus.Counter
	PersistBatchSize      prometheus.Histogram
	PersistErrors         *prometheus.CounterVec
	PersistRetry          prometheus.Counter
	PersistLastSequence   prometheus.Gauge

	// --- Snapshot (§18 §2.8) ---
	SnapshotTaken       prometheus.Counter
	SnapshotDuration    prometheus.Histogram
	SnapshotSizeBytes   prometheus.Gauge
	SnapshotLastSeq     prometheus.Gauge
	ReplayEventsTotal   prometheus.Counter
	ReplayDuration      prometheus.Gauge

	// --- Query API (§18 §2.9) ---
	QueryRequests  *prometheus.CounterVec
	QueryDuration  *prometheus.HistogramVec
	QueryErrors    *prometheus.CounterVec
}

// NewMetrics creates and registers all Prometheus metrics.
func NewMetrics() *Metrics {
	latencyBuckets := []float64{
		0.000001, 0.000005, 0.00001, 0.000025, 0.00005,
		0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01,
	}

	ingestBuckets := []float64{
		0.00001, 0.000025, 0.00005, 0.0001, 0.00025,
		0.0005, 0.001, 0.002, 0.005, 0.01,
	}

	return &Metrics{
		// Core Processing
		CoreEventsApplied: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_core_events_applied_total",
			Help: "Events successfully applied by core",
		}, []string{"event_type"}),

		CoreEventsRejected: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_core_events_rejected_total",
			Help: "Events rejected (dedup, gap, validation)",
		}, []string{"event_type", "reason"}),

		CoreEventDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_core_event_apply_duration_seconds",
			Help:    "Time to apply a single event in core",
			Buckets: latencyBuckets,
		}, []string{"event_type"}),

		CoreJournals: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_core_journals_generated_total",
			Help: "Journal entries generated",
		}, []string{"journal_type"}),

		CoreStateHashDur: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_core_state_hash_duration_seconds",
			Help:    "Time to compute state hash",
			Buckets: latencyBuckets,
		}),

		CoreSequence: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_core_sequence",
			Help: "Current global sequence number",
		}),

		// Latency
		IngestToApply: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_ingest_to_apply_seconds",
			Help:    "NATS receive to core apply complete",
			Buckets: ingestBuckets,
		}, []string{"event_type"}),

		ApplyToPersist: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_apply_to_persist_seconds",
			Help:    "Core emit to Postgres commit",
			Buckets: latencyBuckets,
		}),

		QueryFreshnessLag: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_query_freshness_lag_seconds",
			Help:    "Core sequence minus projection sequence (in time)",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0},
		}, []string{"endpoint"}),

		NATSPullLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_nats_pull_latency_seconds",
			Help:    "NATS pull request latency",
			Buckets: ingestBuckets,
		}, []string{"subject"}),

		PersistBatchDur: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_persist_batch_duration_seconds",
			Help:    "Postgres batch write duration",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),

		ProjectionUpdateDur: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_projection_update_duration_seconds",
			Help:    "Projection table update duration",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1},
		}, []string{"projection"}),

		// Channel & Backpressure
		ChannelSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "perp_channel_size",
			Help: "Current items in channel",
		}, []string{"name"}),

		ChannelCapacity: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "perp_channel_capacity",
			Help: "Channel capacity (constant)",
		}, []string{"name"}),

		ChannelUtilization: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "perp_channel_utilization",
			Help: "Channel size / capacity (0.0-1.0)",
		}, []string{"name"}),

		ProjectionDrops: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_projection_drops_total",
			Help: "Events dropped due to full projection channel",
		}, []string{"projection"}),

		PublishDrops: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_publish_drops_total",
			Help: "Events dropped due to full publish channel",
		}),

		PersistBackpressure: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_persist_backpressure_total",
			Help: "Times core blocked on persist channel",
		}),

		// Idempotency & Ordering
		IdempotencyDuplicates: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_idempotency_duplicates_total",
			Help: "Duplicates caught (lru/postgres)",
		}, []string{"event_type", "tier"}),

		DedupLRUSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_dedup_lru_size",
			Help: "Current LRU occupancy",
		}),

		DedupLRUEvictions: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_dedup_lru_evictions_total",
			Help: "LRU evictions",
		}),

		DedupTier2Duration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_dedup_tier2_duration_seconds",
			Help:    "Postgres dedup lookup latency",
			Buckets: latencyBuckets,
		}),

		EventSequenceGap: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_event_sequence_gap_total",
			Help: "Source sequence gaps",
		}, []string{"partition"}),

		EventOutOfOrder: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_event_out_of_order_total",
			Help: "Out-of-order rejections",
		}, []string{"partition"}),

		// Funding
		FundingEpochSettled: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_funding_epoch_settled_total",
			Help: "Epochs settled",
		}, []string{"market_id"}),

		FundingEpochDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_funding_epoch_duration_seconds",
			Help:    "Time to settle one epoch",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		}, []string{"market_id"}),

		FundingPositionsSettled: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_funding_positions_settled_total",
			Help: "Positions processed per epoch",
		}, []string{"market_id"}),

		FundingTotalPaid: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_funding_total_paid",
			Help: "Total funding paid (absolute, USDT)",
		}, []string{"market_id"}),

		FundingTotalReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_funding_total_received",
			Help: "Total funding received (absolute, USDT)",
		}, []string{"market_id"}),

		FundingRoundingResidual: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "perp_funding_rounding_residual",
			Help: "Rounding residual per epoch",
		}, []string{"market_id"}),

		FundingInsufficientBal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_funding_insufficient_balance_total",
			Help: "Users unable to pay full funding",
		}, []string{"market_id"}),

		// Liquidation
		LiquidationTriggered: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_liquidation_triggered_total",
			Help: "Liquidations triggered",
		}, []string{"market_id"}),

		LiquidationCompleted: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_liquidation_completed_total",
			Help: "Completed (closed/bankrupt)",
		}, []string{"market_id", "outcome"}),

		LiquidationFills: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_liquidation_fills_total",
			Help: "Liquidation fills processed",
		}, []string{"market_id"}),

		LiquidationDeficit: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_liquidation_deficit_total",
			Help: "Total deficit (insurance fund debits)",
		}, []string{"market_id"}),

		InsuranceFundBalance: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_insurance_fund_balance",
			Help: "Current insurance fund balance",
		}),

		// Persistence
		PersistEventsWritten: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_persist_events_written_total",
			Help: "Events written to Postgres",
		}),

		PersistJournalsWritten: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_persist_journals_written_total",
			Help: "Journal entries written to Postgres",
		}),

		PersistBatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_persist_batch_size",
			Help:    "Events per batch",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
		}),

		PersistErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_persist_errors_total",
			Help: "Persistence errors",
		}, []string{"error_type"}),

		PersistRetry: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_persist_retry_total",
			Help: "Persistence retries",
		}),

		PersistLastSequence: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_persist_last_sequence",
			Help: "Last persisted sequence",
		}),

		// Snapshot
		SnapshotTaken: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_snapshot_taken_total",
			Help: "Snapshots created",
		}),

		SnapshotDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "perp_snapshot_duration_seconds",
			Help:    "Snapshot creation time",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0},
		}),

		SnapshotSizeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_snapshot_size_bytes",
			Help: "Last snapshot size",
		}),

		SnapshotLastSeq: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_snapshot_last_sequence",
			Help: "Sequence of last snapshot",
		}),

		ReplayEventsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "perp_replay_events_total",
			Help: "Events replayed on startup",
		}),

		ReplayDuration: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "perp_replay_duration_seconds",
			Help: "Total replay time",
		}),

		// Query API
		QueryRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_query_requests_total",
			Help: "Query requests",
		}, []string{"endpoint", "status"}),

		QueryDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "perp_query_duration_seconds",
			Help:    "Query latency",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		}, []string{"endpoint"}),

		QueryErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "perp_query_errors_total",
			Help: "Query errors",
		}, []string{"endpoint", "code"}),
	}
}

// SetChannelMetrics updates channel utilization metrics.
func (m *Metrics) SetChannelMetrics(name string, size, capacity int) {
	m.ChannelSize.WithLabelValues(name).Set(float64(size))
	m.ChannelCapacity.WithLabelValues(name).Set(float64(capacity))
	if capacity > 0 {
		m.ChannelUtilization.WithLabelValues(name).Set(float64(size) / float64(capacity))
	}
}
