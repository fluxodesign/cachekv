# cachekv

An encrypted key-value store for Go, built on top of [BadgerDB](https://github.com/dgraph-io/badger).

`cachekv` manages a collection of named databases — each optionally encrypted at
rest — behind a single package API. It keeps a **keyring** holding every secure
database's encryption key, a **meta database** recording the catalogue of
databases, configuration, and an audit event log, and a reference-counted
**connection pool** that shares open BadgerDB handles and reclaims idle ones.

## Install

```bash
go get github.com/fluxodesign/cachekv/cachekv
```

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/fluxodesign/cachekv/cachekv"
)

func main() {
	// Initialize (or open) the store. Must be called before anything else.
	cachekv.Startup()

	// Create an encrypted database (pass false for a plain one).
	if err := cachekv.CreateDatabase("users", true); err != nil {
		log.Fatal(err)
	}

	// Write and read entries by database name.
	if err := cachekv.InsertEntry("users", "alice", []byte("hello")); err != nil {
		log.Fatal(err)
	}
	value, err := cachekv.GetEntry("users", "alice")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("alice = %s", value)

	// Or work through a handle. Close releases the pooled connection.
	store, err := cachekv.GetStorageObject("users")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	all, _ := store.All()
	log.Printf("%d entries", len(all))

	// Graceful teardown flushes metrics and closes all connections.
	if err := cachekv.Shutdown(context.Background(), 30*time.Second); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
```

## Configuration

| Setting | How to set | Default |
| --- | --- | --- |
| Store directory | `cachekv.StorePath` (before `Startup`) | `./store/` |
| Keyring directory | `cachekv.KeyPath` (before `Startup`) | `./.private` |
| Idle connection timeout | `POOL_TIMEOUT_MS` env var | `30000` (30s) |

Configuration persisted in the meta database can be read and updated at runtime
with `ListConfigurations` and `UpdateConfigurations`.

## Notes

- Secure databases are encrypted with a per-database key stored in the keyring;
  the keyring itself is encrypted.
- Operations are rejected once `Shutdown` has begun (`IsShuttingDown` reports the
  state).
- Basic in-memory metrics are available via `GetMetricsCollector`.
