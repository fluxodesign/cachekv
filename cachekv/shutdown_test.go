package cachekv

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

func TestShutdownAndWriteEvent(t *testing.T) {
	// Setup similar to db_test.go's setup()
	defer setup()()

	// Capture the current meta database path and key
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	metaKey := loadMetaIdent().key

	// Run Shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := Shutdown(ctx, 2*time.Second)
	assert.Nil(t, err)

	// M2: the shutdown event is written before CloseAll, and CloseAll runs last, so
	// no connection must be left open on the pool afterwards. Under the old ordering
	// (event written after CloseAll) the reopened meta connection leaked into the map.
	pool := GetConnectionPool()
	pool.mu.RLock()
	leaked := len(pool.storages)
	pool.mu.RUnlock()
	assert.Equal(t, 0, leaked, "shutdown left a leaked pool connection")

	// After shutdown, we should be able to verify the shutdown event was written to the meta database.
	// Since Shutdown calls GetConnectionPool().CloseAll(), we need to use a new pool or re-open the DB.
	// Actually, GetConnectionPool() returns a singleton, but CloseAll() clears its internal map.

	db, err := pool.Get(metaPath, metaKey)
	assert.Nil(t, err)
	defer pool.Release(metaPath)

	// Shutdown events are prefixed with prefixMetaEvent + timestamp (ms)
	// We'll search for keys with that prefix
	found := false
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefixMetaEvent)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var event Event
				if err := json.Unmarshal(val, &event); err == nil {
					if event.Comment == "System shutdown" {
						found = true
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	assert.Nil(t, err)
	assert.True(t, found, "Shutdown event not found in meta database")
}
