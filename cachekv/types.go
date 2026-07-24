package cachekv

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// Storage is a handle to a single open database. It bundles the underlying
// BadgerDB connection with its location and encryption key. Because it carries
// an atomic field it must only be used by pointer and never copied.
type Storage struct {
	db   *badger.DB
	path string
	file string
	key  []byte
	// rotatingKey is read and written from multiple goroutines (readers gate on it
	// to bail out during a key rotation), so it must be accessed atomically. Storage
	// must therefore only be used via pointer / the package globals, never copied.
	rotatingKey atomic.Bool
	// poolKey is the exact key this handle acquired from the connection pool
	// (the dbPath passed to pool.Get). Close releases that reference. It is
	// empty when the handle is not pool-managed (e.g. built via NewStorage),
	// in which case Close is a no-op.
	poolKey string
}

// Config holds the store-wide configuration persisted in the meta database.
type Config struct {
	StorePath   string `json:"store_path"`
	SecureNewDb bool   `json:"secure_new_db"`
	MetaStore   string `json:"meta_store"`
	MetaFile    string `json:"meta_file"`
}

// DbObject is the meta-database record describing one database: where it lives,
// whether it is secure/active, and lifecycle timestamps (all in Unix millis).
type DbObject struct {
	DbPath      string `json:"db_path"`
	DbFile      string `json:"db_file"`
	Secure      bool   `json:"secure"`
	Created     int64  `json:"created"`
	Active      bool   `json:"active"`
	LastRotated int64  `json:"last_rotated"`
	Deleted     int64  `json:"deleted"`
}

var shutdownWG sync.WaitGroup
var shutdownFlag int32

// Event is an audit-log entry written to the meta database, recording an
// operation type, a human-readable comment, and a Unix-millis timestamp.
type Event struct {
	Type    EventType `json:"type"`
	Comment string    `json:"comment"`
	TStamp  int64     `json:"tstamp"`
}

// EventType enumerates the kinds of events recorded in the audit log.
type EventType int

// Event types recorded in the audit log (the zero value is unused).
const (
	_ EventType = iota
	EventTypeWrite
	EventTypeRead
	EventTypeCreate
	EventTypeDelete
	EventTypeUpdate
	EventTypeConfigChange
	EventTypeShutdown
	_

	prefixMetaKey    = "metakey:fxstorage"
	prefixMetaDb     = "fxstorage_db:"
	prefixMetaEvent  = "fxstorage_event:"
	prefixMetaConfig = "fxstorage_config"
	lockDb           = "lock.db"
	errDbRotating    = "maintenance: rotating key"
	errDbInactive    = "error: trying to access inactive db"

	// shutdown flag - init as 0 (not shutting down)
	shutdownFlagDefault int32 = 0
)

// metaIdent is the identity of the active meta database: where it lives and the
// key to open it. It is swapped atomically on key rotation so hot-path readers
// never take a lock (and can never observe a torn path/key pair). See H2.
type metaIdent struct {
	path string
	file string
	key  []byte
}

// Global state with mutex protection for thread safety
var (
	globalStateMu sync.RWMutex
	// metaStorage's identity (path/file/key) lives in metaIdentPtr, not in the
	// struct fields — only its .db and .rotatingKey are used for the meta DB.
	metaStorage        Storage
	keyStorage         Storage
	metaIdentPtr       atomic.Pointer[metaIdent]
	fxConfig           *Config
	connectionPool     *ConnectionPool
	connectionPoolOnce sync.Once
)

// EMetaKeyNotFound is returned when a lookup in the meta database finds no entry
// for the requested key. It wraps the underlying badger.ErrKeyNotFound so
// callers can match it with errors.Is / errors.As.
type EMetaKeyNotFound struct {
	Code    int
	Message string
	Wrapped error
}

// Error implements the error interface.
func (e *EMetaKeyNotFound) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("%s (Code: %d)", e.Wrapped.Error(), e.Code)
	}
	return fmt.Sprintf("%s (Code: %d)", e.Message, e.Code)
}

// Unwrap returns the wrapped error, enabling errors.Is / errors.As.
func (e *EMetaKeyNotFound) Unwrap() error {
	return e.Wrapped
}

// GetConnectionPool returns the shared connection pool instance
func GetConnectionPool() *ConnectionPool {
	connectionPoolOnce.Do(func() {
		connectionPool = NewConnectionPool(GetDefaultTimeout())
	})
	return connectionPool
}

// ValidationError represents a validation failure with structured information.
type ValidationError struct {
	Field   string `json:"field"`   // Which field/parameter failed validation
	Code    int    `json:"code"`    // Numeric error code for categorization
	Message string `json:"message"` // Human-readable description of the error
	Wrapped error  `json:"-"`       // Optional underlying error (not serialized to JSON)
}

// Error implements the standard error interface.
func (v *ValidationError) Error() string {
	if v.Wrapped != nil {
		return fmt.Sprintf("%s: %s (code: %d, wrapped: %v)", v.Field, v.Message, v.Code, v.Wrapped)
	}
	return fmt.Sprintf("%s: %s (code: %d)", v.Field, v.Message, v.Code)
}
