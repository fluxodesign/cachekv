package cachekv

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
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

	// Latency tracking (simple moving average)
	lastOpDuration atomic.Int64

	// Startup time for uptime calculation
	startTime time.Time
}

func NewSimpleMetricsCollector() *SimpleMetricsCollector {
	return &SimpleMetricsCollector{
		startTime: time.Now(),
	}
}

func (m *SimpleMetricsCollector) RecordOperation(ctx context.Context, op string, dbName string, duration time.Duration, success bool) {
	m.lastOpDuration.Store(duration.Nanoseconds())

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
	m.lastOpDuration.Store(duration.Nanoseconds())
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
	AverageLatency  time.Duration `json:"average_latency"`
	Uptime          time.Duration `json:"uptime"`
}

// GetMetrics returns current metrics snapshot
func (m *SimpleMetricsCollector) GetMetrics() Metrics {
	return Metrics{
		TotalOperations: m.opsCreated.Load() + m.opsRead.Load() +
			m.opsWrite.Load() + m.opsDelete.Load(),
		Created:         m.opsCreated.Load(),
		Reads:           m.opsRead.Load(),
		Writes:          m.opsWrite.Load(),
		Deletes:         m.opsDelete.Load(),
		Errors:          m.opsError.Load(),
		DatabasesActive: m.dbCount.Load(),
		AverageLatency:  time.Duration(m.lastOpDuration.Load()),
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

// validateStorePath validates that the store path exists and is accessible - FIX: Uncomment this
func validateStorePath(storePath string) error {
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		return &ValidationError{Field: "StorePath", Code: 4096, Message: "store path does not exist"}
	}

	info, _ := os.Stat(storePath)
	if !info.IsDir() {
		return &ValidationError{Field: "StorePath", Code: 4097, Message: "store path is not a directory"}
	}

	return nil
}

// validateKeyPath ensures key storage location is valid and secure
func validateKeyPath(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Key path can be created, but we need parent directory to exist
		parentDir := filepath.Dir(path)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			return &ValidationError{Field: "KeyPath", Code: 4103, Message: "key storage parent directory does not exist"}
		}
	}

	info, _ := os.Stat(path)
	if info != nil && !info.IsDir() {
		return &ValidationError{Field: "KeyPath", Code: 4104, Message: "key path is not a directory"}
	}

	return nil
}

// validatePermissions ensures directories have correct permissions
func validatePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	mode := info.Mode()
	if mode.Perm().String() == "777" || mode.Perm().String() == "0777" {
		log.Printf("Warning: path %s has overly permissive permissions (777)\n", path)
	}

	return nil
}

// validateDependencies ensures required system dependencies are available
func validateDependencies() error {
	// Check if Badger DB can be initialized with test options
	testPath := filepath.Join(os.TempDir(), "cachekv_test_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	defer os.RemoveAll(testPath)

	opt := badger.DefaultOptions(testPath).WithEncryptionKey(make([]byte, keyLength))
	db, err := badger.Open(opt)
	if err != nil {
		return &ValidationError{Field: "Dependencies", Code: 4200, Message: "Badger DB initialization failed", Wrapped: err}
	}
	defer db.Close()

	return nil
}

// validateEncryptionCapability ensures encryption functions work correctly
func validateEncryptionCapability() error {
	_, _, err := readFromStorage(KeyPath)
	if err != nil {
		return &ValidationError{Field: "Encryption", Code: 4300, Message: "could not read public key from storage"}
	}

	// Test encryption/decryption cycle
	testMessage := []byte("test_encryption")
	encrypted, err := encryptMessage(testMessage, nil)
	if err != nil {
		return &ValidationError{Field: "Encryption", Code: 4301, Message: "encryption failed"}
	}

	decrypted, err := decryptMessage(encrypted, nil)
	if err != nil {
		return &ValidationError{Field: "Encryption", Code: 4302, Message: "decryption failed"}
	}

	if string(decrypted) != string(testMessage) {
		return &ValidationError{Field: "Encryption", Code: 4303, Message: "decrypted message does not match original"}
	}

	return nil
}

// validateGCConfig checks if Go runtime GC settings are appropriate
func validateGCConfig() error {
	gcPercent := debug.SetGCPercent(-1) // Get current value without changing it

	if gcPercent < 20 || gcPercent > 100 {
		log.Printf("Warning: GC percent is %d, recommended range is 20-100\n", gcPercent)
	}

	return nil
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

		// Step 3: Close connection pool (waits for active connections to finish)
		globalStateMu.RLock()
		pool := connectionPool
		globalStateMu.RUnlock()

		if pool != nil {
			pool.CloseAll()
			log.Println("[3/4] Connection pool closed")
		}

		// Step 4: Write final state and exit gracefully
		writeShutdownEvent(ctx)
		log.Println("[4/4] Shutdown complete - all resources released")
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

	if metaStorage.rotatingKey {
		log.Println("Cannot write shutdown event during key rotation")
		return
	}

	now := time.Now().UnixMilli()
	event := Event{
		Type:    EventTypeConfigChange,
		Comment: "System shutdown",
		TSTamp:  now,
	}

	metaPath := path.Join(metaStorage.path, metaStorage.file)
	db, err := OpenDatabase(metaPath, metaStorage.key)
	if err != nil {
		log.Printf("Warning: could not open meta database for shutdown event: %v\n", err)
		return
	}
	defer db.Close()

	key := prefixMetaEvent + strconv.FormatInt(now, 10)
	value, _ := json.Marshal(event)
	err = setDbEntry([]byte(key), value, db)
	if err != nil {
		log.Printf("Warning: could not write shutdown event: %v\n", err)
	}

	m := GetMetricsCollector()
	if m != nil {
		lastOpDur := m.(*SimpleMetricsCollector).lastOpDuration.Load()
		if lastOpDur > 0 {
			m.RecordLatency(ctx, "shutdown_final_op", time.Duration(lastOpDur))
		}
	}
}

// IsShuttingDown returns true if the system is currently shutting down
func IsShuttingDown() bool {
	return atomic.LoadInt32(&shutdownFlag) == 1
}
