package cachekv

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Storage struct {
	db          *badger.DB
	path        string
	file        string
	key         []byte
	rotatingKey bool
}

type Config struct {
	StorePath   string `json:"store_path"`
	SecureNewDb bool   `json:"secure_new_db"`
	MetaStore   string `json:"meta_store"`
	MetaFile    string `json:"meta_file"`
}

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

type Event struct {
	Type    EventType `json:"type"`
	Comment string    `json:"comment"`
	TStamp  int64     `json:"tstamp"`
}

type EventType int

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

// Global state with mutex protection for thread safety
var (
	globalStateMu      sync.RWMutex
	metaStorage        Storage
	keyStorage         Storage
	fxConfig           *Config
	connectionPool     *ConnectionPool
	connectionPoolOnce sync.Once
)

type EMetaKeyNotFound struct {
	Code    int
	Message string
	Wrapped error
}

func (e *EMetaKeyNotFound) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("%s (Code: %d)", e.Message, e.Code)
	}
	return fmt.Sprintf("%s (Code: %d)", e.Message, e.Code)
}

func (e *EMetaKeyNotFound) Unwrap() error {
	return e.Wrapped
}

// GetConnectionPool returns the shared connection pool instance
func GetConnectionPool() *ConnectionPool {
	connectionPoolOnce.Do(func() {
		connectionPool = NewConnectionPool(5 * time.Minute)
	})
	return connectionPool
}

// ValidationError represents a validation failure with structured information - ADD THIS NEW TYPE
type ValidationError struct {
	Field   string `json:"field"`   // Which field/parameter failed validation
	Code    int    `json:"code"`    // Numeric error code for categorization
	Message string `json:"message"` // Human-readable description of the error
	Wrapped error  `json:"-"`       // Optional underlying error (not serialized to JSON)
}

// Error implements the standard error interface - REQUIRED FOR ERROR TYPE
func (v *ValidationError) Error() string {
	if v.Wrapped != nil {
		return fmt.Sprintf("%s: %s (code: %d, wrapped: %v)", v.Field, v.Message, v.Code, v.Wrapped)
	}
	return fmt.Sprintf("%s: %s (code: %d)", v.Field, v.Message, v.Code)
}
