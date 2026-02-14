package core

import (
	"container/list"
	"fmt"
)

// IdempotencyChecker implements two-tier deduplication
type IdempotencyChecker struct {
	// Tier 1: In-memory LRU
	lru *IdempotencyLRU

	// Tier 2: Postgres (injected via interface)
	dbChecker DBIdempotencyChecker

	// Metrics
	metrics *IdempotencyMetrics
}

// DBIdempotencyChecker is the interface for Postgres dedup lookup
type DBIdempotencyChecker interface {
	IsDuplicate(eventType string, idempotencyKey string) (bool, error)
}

func NewIdempotencyChecker(capacity int, dbChecker DBIdempotencyChecker) *IdempotencyChecker {
	return &IdempotencyChecker{
		lru:       NewIdempotencyLRU(capacity),
		dbChecker: dbChecker,
		metrics:   NewIdempotencyMetrics(),
	}
}

// IsDuplicate checks if event has been processed (two-tier lookup)
func (ic *IdempotencyChecker) IsDuplicate(eventType string, idempotencyKey string) bool {
	compositeKey := fmt.Sprintf("%s:%s", eventType, idempotencyKey)

	// Tier 1: LRU check (hot path)
	if ic.lru.Contains(compositeKey) {
		ic.metrics.RecordDuplicate(eventType, "lru")
		return true
	}

	// Tier 2: Postgres check (cold path)
	if ic.dbChecker != nil {
		isDup, err := ic.dbChecker.IsDuplicate(eventType, idempotencyKey)
		if err != nil {
			// Log error but don't fail - conservative: assume not duplicate
			// This prevents a DB issue from blocking event processing
			ic.metrics.RecordTier2Error()
			return false
		}

		if isDup {
			ic.metrics.RecordDuplicate(eventType, "postgres")
			// Add to LRU so we don't hit DB again
			ic.lru.Add(compositeKey)
			return true
		}
	}

	return false
}

// MarkProcessed adds key to LRU after successful processing
func (ic *IdempotencyChecker) MarkProcessed(eventType string, idempotencyKey string) {
	compositeKey := fmt.Sprintf("%s:%s", eventType, idempotencyKey)
	ic.lru.Add(compositeKey)
}

// GetMetrics returns metrics for monitoring
func (ic *IdempotencyChecker) GetMetrics() *IdempotencyMetrics {
	return ic.metrics
}

// --- LRU Implementation ---

// IdempotencyLRU is an LRU cache for idempotency keys.
// Not thread-safe — only accessed from the single-threaded deterministic core.
type IdempotencyLRU struct {
	capacity int
	cache    map[string]*list.Element
	lruList  *list.List

	evictions int64 // For metrics
}

type lruEntry struct {
	key string
}

func NewIdempotencyLRU(capacity int) *IdempotencyLRU {
	return &IdempotencyLRU{
		capacity: capacity,
		cache:    make(map[string]*list.Element, capacity),
		lruList:  list.New(),
	}
}

// Contains checks if key exists (promotes to front)
func (lru *IdempotencyLRU) Contains(key string) bool {
	elem, exists := lru.cache[key]
	if exists {
		// Move to front (most recently used)
		lru.lruList.MoveToFront(elem)
		return true
	}
	return false
}

// Add inserts a key (or promotes if exists)
func (lru *IdempotencyLRU) Add(key string) {
	// Check if already exists
	if elem, exists := lru.cache[key]; exists {
		lru.lruList.MoveToFront(elem)
		return
	}

	// Add new entry
	entry := &lruEntry{key: key}
	elem := lru.lruList.PushFront(entry)
	lru.cache[key] = elem

	// Evict if over capacity
	if lru.lruList.Len() > lru.capacity {
		lru.evictOldest()
	}
}

func (lru *IdempotencyLRU) evictOldest() {
	elem := lru.lruList.Back()
	if elem != nil {
		lru.lruList.Remove(elem)
		entry := elem.Value.(*lruEntry)
		delete(lru.cache, entry.key)
		lru.evictions++
	}
}

// WarmFromKeys loads a batch of composite keys into the LRU.
// Per doc §10: on restart, load recent idempotency keys from Postgres
// into the LRU to avoid cold-path DB lookups for recently processed events.
func (lru *IdempotencyLRU) WarmFromKeys(keys []string) {
	for _, key := range keys {
		if _, exists := lru.cache[key]; exists {
			continue
		}
		entry := &lruEntry{key: key}
		elem := lru.lruList.PushFront(entry)
		lru.cache[key] = elem

		if lru.lruList.Len() > lru.capacity {
			lru.evictOldest()
		}
	}
}

// Size returns current number of entries
func (lru *IdempotencyLRU) Size() int {
	return lru.lruList.Len()
}

// Evictions returns total evictions (for metrics)
func (lru *IdempotencyLRU) Evictions() int64 {
	return lru.evictions
}

// --- Metrics ---

// IdempotencyMetrics tracks dedup stats.
// Not thread-safe — only accessed from the single-threaded deterministic core.
type IdempotencyMetrics struct {
	duplicatesLRU      map[string]int64 // event_type -> count
	duplicatesPostgres map[string]int64
	tier2Errors        int64
}

func NewIdempotencyMetrics() *IdempotencyMetrics {
	return &IdempotencyMetrics{
		duplicatesLRU:      make(map[string]int64),
		duplicatesPostgres: make(map[string]int64),
	}
}

func (m *IdempotencyMetrics) RecordDuplicate(eventType string, tier string) {
	if tier == "lru" {
		m.duplicatesLRU[eventType]++
	} else {
		m.duplicatesPostgres[eventType]++
	}
}

func (m *IdempotencyMetrics) RecordTier2Error() {
	m.tier2Errors++
}

func (m *IdempotencyMetrics) GetDuplicates(eventType string) (lru int64, postgres int64) {
	return m.duplicatesLRU[eventType], m.duplicatesPostgres[eventType]
}

func (m *IdempotencyMetrics) GetTier2Errors() int64 {
	return m.tier2Errors
}
