package cachekv

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ConnectionPool manages persistent Badger DB connections with lifecycle management
type ConnectionPool struct {
	mu       sync.RWMutex
	storages map[string]*poolEntry
	timeout  time.Duration // How long before closing idle connections
}

type poolEntry struct {
	db         *badger.DB
	lastAccess time.Time
	refCount   int
}

// NewConnectionPool creates a new connection pool with configurable timeout
func NewConnectionPool(timeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		storages: make(map[string]*poolEntry),
		timeout:  timeout,
	}
}

// Get retrieves or creates a database connection for the given path and key
func (p *ConnectionPool) Get(dbPath string, key []byte) (*badger.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry := p.storages[dbPath]
	if entry != nil && !entry.db.IsClosed() {
		entry.refCount++
		entry.lastAccess = time.Now()
		return entry.db, nil
	}

	// Create new connection
	db, err := OpenDatabase(dbPath, key)
	if err != nil {
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
	case <-context.Background().Done():
		return
	}
}

// CloseAll closes all managed connections immediately
func (p *ConnectionPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for path, entry := range p.storages {
		if !entry.db.IsClosed() {
			err := entry.db.Close()
			if err != nil {
				log.Printf("Error closing database %s: %v", path, err)
			}
			delete(p.storages, path)
		}
	}

	p.storages = make(map[string]*poolEntry)
}
