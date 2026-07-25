package cachekv

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collector global storage
var metricsCollector *SimpleMetricsCollector = nil
var collectorMu sync.RWMutex

// MetricsCollector interface for pluggable metrics implementation
type MetricsCollector interface {
	// Database operations
	RecordOperation(ctx context.Context, op string, dbName string, duration time.Duration, success bool)
	RecordDatabaseCreated(dbName string)
	RecordDatabaseDeleted(dbName string)

	// Key operations
	RecordKeyRotation(sourceFile string, targetFile string, itemCount int)
	RecordEncryptionError(reason string)

	// System metrics
	RecordMemoryUsage(bytesUsed uint64)
	RecordGCStats(gcCount uint32, pauseTime time.Duration)

	// Latency tracking
	RecordLatency(ctx context.Context, operation string, duration time.Duration)

	// Close the collector and flush remaining metrics
	Close() error
}

// NoOpMetricsCollector is a no-op implementation for when metrics are disabled
type NoOpMetricsCollector struct{}

func (n *NoOpMetricsCollector) RecordOperation(ctx context.Context, op string, dbName string, duration time.Duration, success bool) {
	// No-op
}

func (n *NoOpMetricsCollector) RecordDatabaseCreated(dbName string)                            {}
func (n *NoOpMetricsCollector) RecordDatabaseDeleted(dbName string)                            {}
func (n *NoOpMetricsCollector) RecordKeyRotation(sourceFile, targetFile string, itemCount int) {}
func (n *NoOpMetricsCollector) RecordEncryptionError(reason string)                            {}
func (n *NoOpMetricsCollector) RecordMemoryUsage(bytesUsed uint64)                             {}
func (n *NoOpMetricsCollector) RecordGCStats(gcCount uint32, pauseTime time.Duration)          {}
func (n *NoOpMetricsCollector) RecordLatency(ctx context.Context, operation string, duration time.Duration) {
}
func (n *NoOpMetricsCollector) Close() error { return nil }

// latencyRingSize bounds how many recent latency samples are kept for
// percentile calculation. Older samples are overwritten in place, so
// percentiles reflect the most recent window rather than the entire
// process lifetime (the mean, tracked separately via a running sum/count,
// does cover the entire lifetime).
const latencyRingSize = 1024

// latencyStats tracks an exact running mean (via atomic sum/count) plus a
// bounded recent-sample window used to estimate p50/p95/p99. It avoids an
// external histogram dependency at the cost of percentiles being windowed
// rather than exact-over-all-time.
type latencyStats struct {
	count atomic.Uint64
	sum   atomic.Uint64 // nanoseconds
	last  atomic.Int64  // nanoseconds of the most recently recorded sample

	mu      sync.Mutex
	samples [latencyRingSize]int64
	next    int
	filled  bool
}

func (l *latencyStats) record(d time.Duration) {
	ns := d.Nanoseconds()
	l.count.Add(1)
	l.sum.Add(uint64(ns))
	l.last.Store(ns)

	l.mu.Lock()
	l.samples[l.next] = ns
	l.next++
	if l.next == latencyRingSize {
		l.next = 0
		l.filled = true
	}
	l.mu.Unlock()
}

func (l *latencyStats) mean() time.Duration {
	c := l.count.Load()
	if c == 0 {
		return 0
	}
	return time.Duration(l.sum.Load() / c)
}

// percentiles returns the p50, p95, and p99 latency over the current sample
// window using nearest-rank interpolation.
func (l *latencyStats) percentiles() (p50, p95, p99 time.Duration) {
	l.mu.Lock()
	n := l.next
	if l.filled {
		n = latencyRingSize
	}
	if n == 0 {
		l.mu.Unlock()
		return 0, 0, 0
	}
	snapshot := make([]int64, n)
	copy(snapshot, l.samples[:n])
	l.mu.Unlock()

	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i] < snapshot[j] })
	rank := func(p float64) time.Duration {
		idx := int(p * float64(len(snapshot)-1))
		return time.Duration(snapshot[idx])
	}
	return rank(0.50), rank(0.95), rank(0.99)
}

// SimpleMetricsCollector provides basic in-memory metrics tracking with counters and timers
type SimpleMetricsCollector struct {
	// Operation counters (atomic for thread safety)
	opsCreated atomic.Uint64
	opsRead    atomic.Uint64
	opsWrite   atomic.Uint64
	opsDelete  atomic.Uint64
	opsError   atomic.Uint64

	// Database lifecycle
	dbCount atomic.Int64

	// Latency tracking: exact mean plus windowed p50/p95/p99.
	latency latencyStats

	// Startup time for uptime calculation
	startTime time.Time
}

// NewSimpleMetricsCollector returns a SimpleMetricsCollector with its uptime
// clock started at the current time.
func NewSimpleMetricsCollector() *SimpleMetricsCollector {
	return &SimpleMetricsCollector{
		startTime: time.Now(),
	}
}

func (m *SimpleMetricsCollector) RecordOperation(ctx context.Context, op string, dbName string, duration time.Duration, success bool) {
	m.latency.record(duration)

	switch op {
	case "create":
		m.opsCreated.Add(1)
	case "read":
		m.opsRead.Add(1)
	case "write", "insert":
		m.opsWrite.Add(1)
	case "delete":
		m.opsDelete.Add(1)
	default:
		if !success {
			m.opsError.Add(1)
		}
	}
}

func (m *SimpleMetricsCollector) RecordDatabaseCreated(dbName string) {
	m.dbCount.Add(1)
}

func (m *SimpleMetricsCollector) RecordDatabaseDeleted(dbName string) {
	m.dbCount.Add(-1)
}

func (m *SimpleMetricsCollector) RecordKeyRotation(sourceFile, targetFile string, itemCount int) {
	// Track rotation events in production with external metrics system
}

func (m *SimpleMetricsCollector) RecordEncryptionError(reason string) {
	m.opsError.Add(1)
}

func (m *SimpleMetricsCollector) RecordMemoryUsage(bytesUsed uint64)                    {}
func (m *SimpleMetricsCollector) RecordGCStats(gcCount uint32, pauseTime time.Duration) {}
func (m *SimpleMetricsCollector) RecordLatency(ctx context.Context, operation string, duration time.Duration) {
	m.latency.record(duration)
}

func (m *SimpleMetricsCollector) Close() error { return nil }

// Metrics provides read-only access to current metrics state
type Metrics struct {
	TotalOperations uint64        `json:"total_operations"`
	Created         uint64        `json:"created"`
	Reads           uint64        `json:"reads"`
	Writes          uint64        `json:"writes"`
	Deletes         uint64        `json:"deletes"`
	Errors          uint64        `json:"errors"`
	DatabasesActive int64         `json:"databases_active"`
	MeanLatency     time.Duration `json:"mean_latency"`
	P50Latency      time.Duration `json:"p50_latency"`
	P95Latency      time.Duration `json:"p95_latency"`
	P99Latency      time.Duration `json:"p99_latency"`
	Uptime          time.Duration `json:"uptime"`
}

// GetMetrics returns current metrics snapshot
func (m *SimpleMetricsCollector) GetMetrics() Metrics {
	p50, p95, p99 := m.latency.percentiles()
	return Metrics{
		TotalOperations: m.opsCreated.Load() + m.opsRead.Load() +
			m.opsWrite.Load() + m.opsDelete.Load(),
		Created:         m.opsCreated.Load(),
		Reads:           m.opsRead.Load(),
		Writes:          m.opsWrite.Load(),
		Deletes:         m.opsDelete.Load(),
		Errors:          m.opsError.Load(),
		DatabasesActive: m.dbCount.Load(),
		MeanLatency:     m.latency.mean(),
		P50Latency:      p50,
		P95Latency:      p95,
		P99Latency:      p99,
		Uptime:          time.Since(m.startTime),
	}
}

// GetMetricsCollector returns the global metrics collector instance
func GetMetricsCollector() MetricsCollector {
	collectorMu.RLock()
	defer collectorMu.RUnlock()
	return metricsCollector
}

// SetMetricsCollector sets the global metrics collector instance
func SetMetricsCollector(collector *SimpleMetricsCollector) {
	collectorMu.Lock()
	defer collectorMu.Unlock()
	metricsCollector = collector
}

// Shutdown closes all resources in the correct order with timeout support
func Shutdown(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	shutdownWG.Add(1)
	go func() {
		defer shutdownWG.Done()

		log.Println("Initiating graceful shutdown...")

		// Step 1: Stop accepting new connections
		atomic.StoreInt32(&shutdownFlag, 1)
		log.Println("[/4] Shutdown flag set - no new operations accepted")

		// Step 2: Flush in-flight metrics
		m := GetMetricsCollector()
		if m != nil {
			if closer, ok := m.(io.Closer); ok {
				err := closer.Close()
				if err != nil {
					log.Printf("Warning closing metrics collector: %v\n", err)
				}
			}
		}
		log.Println("[2/4] Metrics flushed")

		// Step 3: Write final state while the pool is still open. This must happen
		// BEFORE CloseAll — otherwise writeShutdownEvent reopens the meta DB on a
		// pool whose context is already cancelled, leaking a connection that never
		// gets closed before exit (M2).
		writeShutdownEvent(ctx)
		log.Println("[3/4] Final state written")

		// Step 4: Close connection pool last (waits for active connections to finish).
		globalStateMu.RLock()
		pool := connectionPool
		globalStateMu.RUnlock()

		if pool != nil {
			pool.CloseAll()
			log.Println("[4/4] Connection pool closed - all resources released")
		}
	}()

	// Wait for shutdown to complete or timeout
	done := make(chan struct{})
	go func() {
		shutdownWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Graceful shutdown completed successfully")
		return nil
	case <-ctx.Done():
		log.Printf("Shutdown timeout after %v\n", timeout)
		return ctx.Err()
	}
}

// writeShutdownEvent logs the shutdown event for audit trail
func writeShutdownEvent(ctx context.Context) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()

	if metaStorage.rotatingKey.Load() {
		log.Println("Cannot write shutdown event during key rotation")
		return
	}

	now := time.Now().UnixMilli()
	event := Event{
		Type:    EventTypeConfigChange,
		Comment: "System shutdown",
		TStamp:  now,
	}

	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		log.Printf("Warning: could not open meta database for shutdown event: %v\n", err)
		return
	}
	defer pool.Release(metaPath)

	key := prefixMetaEvent + strconv.FormatInt(now, 10)
	value, _ := json.Marshal(event)
	err = setDbEntry([]byte(key), value, db)
	if err != nil {
		log.Printf("Warning: could not write shutdown event: %v\n", err)
	}

	m := GetMetricsCollector()
	if m != nil {
		lastOpDur := m.(*SimpleMetricsCollector).latency.last.Load()
		if lastOpDur > 0 {
			m.RecordLatency(ctx, "shutdown_final_op", time.Duration(lastOpDur))
		}
	}
}

// IsShuttingDown returns true if the system is currently shutting down
func IsShuttingDown() bool {
	return atomic.LoadInt32(&shutdownFlag) == 1
}
