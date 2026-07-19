package cachekv

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ConnectionPool manages persistent Badger DB connections with lifecycle management
type ConnectionPool struct {
	mu       sync.RWMutex
	storages map[string]*poolEntry
	timeout  time.Duration // How long before closing idle connections
	ctx      context.Context
	cancel   context.CancelFunc
}

const defaultPoolTimeout = 30 * time.Second

type poolEntry struct {
	db         *badger.DB
	lastAccess time.Time
	refCount   int
}

// NewConnectionPool creates a new connection pool with configurable timeout
func NewConnectionPool(timeout time.Duration) *ConnectionPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConnectionPool{
		storages: make(map[string]*poolEntry),
		timeout:  timeout,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// GetDefaultTimeout returns the configured timeout based on environment
func GetDefaultTimeout() time.Duration {
	if envTimeout := os.Getenv("POOL_TIMEOUT_MS"); envTimeout != "" {
		val, err := strconv.Atoi(envTimeout)
		if err == nil && val > 0 {
			return time.Duration(val) * time.Millisecond
		}
	}
	return defaultPoolTimeout
}

// Get retrieves or creates a database connection for the given path and key
func (p *ConnectionPool) Get(dbPath string, key []byte) (*badger.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry := p.storages[dbPath]
	if entry != nil && !entry.db.IsClosed() {
		// If an encryption key is provided, Badger will fail to open if it doesn't match the existing one.
		// However, if the DB is already open, we should check if the provided key matches the one it was opened with.
		// Since Badger doesn't expose the key easily, and our pool assumes one connection per path,
		// we try to "re-open" it conceptually by calling OpenDatabase, which will fail if the key is wrong.
		// But if it's already open, we can't really "re-open" it to check the key without closing it.
		// The requirement of TestDifferentEncryptionKeys is to verify that opening with a wrong key fails.

		// If the DB is already open, and a key is provided, we check if it matches the encryption key of the open DB.
		// Badger options contain the EncryptionKey.
		// Note: We only check if BOTH have keys. If one doesn't, we skip this check and let Badger handle it if it tries to re-open.
		// However, in our pool, if it's already open, we assume it's the same DB.
		// TestDifferentEncryptionKeys expects a failure when a different key is provided for an ALREADY OPEN DB.
		if len(key) > 0 {
			opts := entry.db.Opts()
			if len(opts.EncryptionKey) > 0 && !bytes.Equal(key, opts.EncryptionKey) {
				return nil, errors.New("encryption key mismatch for already open database")
			}
		}

		entry.refCount++
		entry.lastAccess = time.Now()
		return entry.db, nil
	}

	// Create new connection
	db, err := OpenDatabase(dbPath, key)
	if err != nil {
		log.Printf("Error opening database %s: %v", dbPath, err)
		return nil, err
	}

	p.storages[dbPath] = &poolEntry{
		db:         db,
		lastAccess: time.Now(),
		refCount:   1,
	}

	return db, nil
}

// Release decreases the reference count for a connection
func (p *ConnectionPool) Release(dbPath string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry := p.storages[dbPath]
	if entry == nil {
		return
	}

	entry.refCount--
	if entry.refCount <= 0 {
		// Mark for cleanup but don't close immediately
		entry.refCount = 0
		go p.scheduleCleanup(dbPath)
	}
}

// scheduleCleanup delays closing idle connections to allow concurrent access
func (p *ConnectionPool) scheduleCleanup(dbPath string) {
	select {
	case <-time.After(p.timeout):
		p.mu.Lock()
		defer p.mu.Unlock()

		entry := p.storages[dbPath]
		if entry != nil && !entry.db.IsClosed() && time.Since(entry.lastAccess) >= p.timeout {
			err := entry.db.Close()
			if err != nil {
				log.Printf("Error closing database %s: %v", dbPath, err)
			}
			delete(p.storages, dbPath)
		}
	case <-p.ctx.Done():
		return
	}
}

// CloseAll closes all managed connections immediately and cancels pending cleanups
func (p *ConnectionPool) CloseAll() {
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	for path, entry := range p.storages {
		if !entry.db.IsClosed() {
			err := entry.db.Close()
			if err != nil {
				log.Printf("Error closing database %s: %v", path, err)
			}
		}
	}

	p.storages = make(map[string]*poolEntry)
}
