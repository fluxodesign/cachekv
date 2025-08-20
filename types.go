package cachekv

import "github.com/dgraph-io/badger/v4"

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

type Event struct {
	Type    EventType `json:"type"`
	Comment string    `json:"comment"`
	TSTamp  int64     `json:"tstamp"`
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
	_

	prefixMetaKey    = "metakey:fxstorage"
	prefixMetaDb     = "fxstorage_db:"
	prefixMetaEvent  = "fxstorage_event:"
	prefixMetaConfig = "fxstorage_config"
	lockDb           = "lock.db"
	errDbRotating    = "maintenance: rotating key"
	errDbInactive    = "error: trying to access inactive db"
)
