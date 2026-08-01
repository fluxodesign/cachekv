package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/fluxodesign/cachekv/cachekv"
)

const (
	stringPrefix = "s:"
	hashPrefix   = "h:"
	setPrefix    = "z:"
)

// snapshot flattens store's three maps into a single key->value set suitable
// for cachekv.BatchInsert, prefixing each key by its logical type so they can
// share one namespace without colliding.
func snapshot(store *Datastore) map[string][]byte {
	entries := make(map[string][]byte, len(store.Strings)+len(store.Hashes)+len(store.Sets))

	for key, val := range store.Strings {
		entries[stringPrefix+key] = []byte(val)
	}
	for key, fields := range store.Hashes {
		if b, err := json.Marshal(fields); err == nil {
			entries[hashPrefix+key] = b
		}
	}
	for key, members := range store.Sets {
		names := make([]string, 0, len(members))
		for member := range members {
			names = append(names, member)
		}
		if b, err := json.Marshal(names); err == nil {
			entries[setPrefix+key] = b
		}
	}
	return entries
}

// saveToDisk persists store's current contents to the named cachekv database
// in a single batch, mirroring one Redis-style RDB snapshot.
func saveToDisk(dbName string, store *Datastore) error {
	return cachekv.BatchInsert(dbName, snapshot(store))
}

// runPeriodicSave sends an internal "SAVE" pseudo-command every interval.
// stateProcessor decides whether enough has changed to actually write to
// disk, so idle ticks are cheap no-ops; the send itself is fire-and-forget
// (nil Resp).
func runPeriodicSave(interval time.Duration, cmdChannel chan<- CkvCommand) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		cmdChannel <- CkvCommand{Op: "SAVE"}
	}
}

// loadFromDisk reconstructs a *Datastore from whatever was last persisted to
// the named cachekv database, reversing the prefixing done by snapshot.
func loadFromDisk(dbName string) (*Datastore, error) {
	storage, err := cachekv.GetStorageObject(dbName)
	if err != nil {
		return nil, err
	}
	defer storage.Close()

	all, err := storage.All()
	if err != nil {
		return nil, err
	}

	store := NewDatastore()
	for key, val := range all {
		switch {
		case strings.HasPrefix(key, stringPrefix):
			store.Strings[key[len(stringPrefix):]] = string(val)
		case strings.HasPrefix(key, hashPrefix):
			fields := make(map[string]string)
			if err := json.Unmarshal(val, &fields); err == nil {
				store.Hashes[key[len(hashPrefix):]] = fields
			}
		case strings.HasPrefix(key, setPrefix):
			var names []string
			if err := json.Unmarshal(val, &names); err == nil {
				members := make(map[string]struct{}, len(names))
				for _, name := range names {
					members[name] = struct{}{}
				}
				store.Sets[key[len(setPrefix):]] = members
			}
		}
	}
	return store, nil
}
