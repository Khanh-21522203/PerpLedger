package observability

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// HealthChecker manages liveness and readiness state.
// Per doc §20 §7: /healthz (liveness), /readyz (readiness).
type HealthChecker struct {
	ready     atomic.Bool
	startTime time.Time
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		startTime: time.Now(),
	}
}

// SetReady marks the service as ready to accept traffic.
func (h *HealthChecker) SetReady(ready bool) {
	h.ready.Store(ready)
}

// IsReady returns whether the service is ready.
func (h *HealthChecker) IsReady() bool {
	return h.ready.Load()
}

// LivenessHandler returns HTTP 200 if the process is alive.
// Per doc §20: liveness probe — always returns OK if process is running.
func (h *HealthChecker) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "alive",
		"uptime": time.Since(h.startTime).String(),
	})
}

// ReadinessHandler returns HTTP 200 if the service is ready, 503 otherwise.
// Per doc §20: readiness probe — returns OK only after recovery complete,
// DB connected, NATS connected, and initial replay done.
func (h *HealthChecker) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if h.ready.Load() {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ready",
		})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "not_ready",
		})
	}
}
