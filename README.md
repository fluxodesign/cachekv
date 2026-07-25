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

## gRPC server and CLI

`cmd/cachekv-server` exposes the package API over gRPC, and `cmd/cachekv-cli`
talks to it.

> **No TLS, no authentication.** The server listens in plaintext and accepts any
> caller. Only run it on a trusted network or loopback interface.

```bash
go run ./cmd/cachekv-server --listen :50051 --store-path ./store --key-path ./.private
```

The CLI takes its global flags *before* the subcommand:

```bash
cachekv-cli [--addr host:port] [--json] [--timeout 30s] <command> [arguments]
```

`--addr` defaults to `$CACHEKV_ADDR`, then `localhost:50051`.

| Command | Description |
| --- | --- |
| `db create <name> [--secure]` | Create a database, optionally encrypted |
| `db list` | List database names |
| `put <db> <key> <value>` | Insert a new entry |
| `update <db> <key> <value>` | Overwrite an existing entry |
| `rm <db> <key>` | Remove an entry |
| `batch-insert <db> --file entries.json` | Insert many entries in one call |
| `get <db> <key>` | Print one value |
| `all <db>` | Stream every entry as `key<TAB>value` |
| `config get` | Print the persisted configuration |
| `config set <json-file>` | Patch the persisted configuration |
| `metrics` | Print the server's metrics snapshot |
| `ping` | Check that the server answers |

`batch-insert --file` (or `--file -` for stdin) reads an array of entries;
`encoding` is optional and defaults to `utf8`:

```json
[
  {"key": "alpha", "value": "one"},
  {"key": "beta", "value": "AQIDBA==", "encoding": "base64"}
]
```

`config set` takes only the keys you want to change — omitted keys keep their
current server-side value:

```bash
echo '{"secure_new_db": true}' | cachekv-cli config set -
```

With `--json`, commands print one JSON object; `all` prints one object per line
so it can be consumed while the stream is still running. Values that are not
valid UTF-8 come back base64-encoded, tagged by an `encoding` field (and prefixed
`base64:` in plain-text output). Exit status is `0` on success, `2` for a usage
error, and `1` for anything else.

Regenerate the protobuf code after editing `proto/cachekv/v1/cachekv.proto`:

```bash
make proto
```

## Notes

- Secure databases are encrypted with a per-database key stored in the keyring;
  the keyring itself is encrypted.
- Operations are rejected once `Shutdown` has begun (`IsShuttingDown` reports the
  state).
- Basic in-memory metrics are available via `GetMetricsCollector`.
