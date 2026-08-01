# Periodic disk-persistence for the in-memory cache store

## Context

`cmd/cachekv-server`'s "cache store" mode (`--run-as-cachestore=true`) runs a
Redis-like, single-threaded in-memory `Datastore` (`Strings`/`Hashes`/`Sets` maps
in `datastore.go`) reachable over a raw TCP listener. It has no durability at
all today — a restart wipes everything. The library already sitting right next
to it, `cachekv`, is an encrypted, durable KV store with exactly the primitives
needed to fix this (`CreateDatabase`, `BatchInsert`, `GetStorageObject`,
`(*Storage).All`). The ask is to periodically flush the in-memory store to disk
through those existing `cachekv` functions, mimicking Redis's RDB model:
snapshot on an interval (only if something changed), snapshot on clean
shutdown, and reload the snapshot back into memory on startup.

## Key files

- `cmd/cachekv-server/datastore.go` — `Datastore`, `CkvCommand`, `stateProcessor`,
  `commandRegistry`. All mutation of `Datastore` happens on the single
  `stateProcessor` goroutine — no locks. Any persistence code must not read
  those maps from a second goroutine.
- `cmd/cachekv-server/main.go` — flags, the `if *runAsCachestore` block that
  starts `stateProcessor` + the raw-protocol listener, and the shutdown
  sequence (grpc drain → `cachekv.Shutdown` → `os.Exit(0)`).
- `cachekv/db.go` — `CreateDatabase`, `BatchInsert(dbName, map[string][]byte)`,
  `GetStorageObject`/`(*Storage).All()`, `GetEntry`. These are the "usual
  cachekv db functions" to reuse rather than inventing a new file format.
- `cachekv/metrics.go` `Shutdown()` — sets the shutting-down flag as step 1 and
  closes the connection pool (`pool.CloseAll()`) as the last step. This means
  a final save **must** run and complete before `cachekv.Shutdown(...)` is
  called in `main()`, not after.

## Design

### 1. Persistence schema

One dedicated `cachekv` database, name configurable via a new `--persist-db`
flag (default `"cachestore"`), created once at startup with
`cachekv.CreateDatabase(name, false)` (ignore the "already exists" error —
same idempotent-create pattern already used implicitly elsewhere).

`cachekv` is a flat `key → []byte` store, but `Datastore` has three separate
Go maps. To flatten them into one namespace without collisions, prefix each
persisted key by logical type:
- `"s:" + key` → raw string value (`Strings`)
- `"h:" + key` → `json.Marshal(map[string]string)` (`Hashes`)
- `"z:" + key` → `json.Marshal([]string)` of the set's members (`Sets`)

JSON matches how the rest of `cachekv` already encodes structured values
(`DbObject`, `Config`, `Event` all use `encoding/json`).

### 2. New file: `cmd/cachekv-server/persistence.go`

- `snapshot(store *Datastore) map[string][]byte` — pure function building the
  prefixed entries above.
- `saveToDisk(dbName string, store *Datastore) error` — one
  `cachekv.BatchInsert(dbName, snapshot(store))` call (single batch write,
  mirrors one RDB snapshot).
- `loadFromDisk(dbName string) (*Datastore, error)` — `cachekv.GetStorageObject`
  → `.All()` → demultiplex by prefix into a fresh `NewDatastore()`
  (JSON-decoding hash/set values), `defer storage.Close()`.

### 3. Wiring through the single-threaded processor (no new locks)

Reuse the existing `CkvCommand{Op, Args, Resp}` shape: reserve `Op == "SAVE"`
as an internal pseudo-command, handled specially inside `stateProcessor`
*before* the `commandRegistry` lookup:
- A `dirty` counter, local to `stateProcessor`, increments after any
  successful write command (`SET`/`HSET`/`SADD`).
- On `Op == "SAVE"`: if `Args` contains `"force"` OR `dirty >= saveMinChanges`,
  call `saveToDisk`, reset `dirty = 0`, reply `+OK\r\n` (or the error) on
  `Resp` if non-nil. Otherwise no-op (cheap skip on idle ticks).

`stateProcessor`'s signature changes to accept the initial store:
`stateProcessor(cmdChannel <-chan CkvCommand, store *Datastore)` instead of
constructing it internally — needed so `main()` can hand it the result of
`loadFromDisk` on startup.

A ticker goroutine, `runPeriodicSave(interval time.Duration, cmdChannel chan<- CkvCommand)`,
started alongside `stateProcessor`, sends `CkvCommand{Op: "SAVE"}` (nil `Resp`,
fire-and-forget) every tick.

### 4. Startup reload + shutdown save, wired in `main()`

- When `*runAsCachestore`: call `cachekv.CreateDatabase(*persistDb, false)`
  (ignore already-exists), then `loadFromDisk(*persistDb)` for the initial
  store (fall back to `NewDatastore()` if nothing persisted yet). Hoist
  `cmdChannel` to a `main()`-level variable (currently scoped inside the `if`
  block) so the shutdown sequence can reach it.
- In the existing shutdown sequence, **after** grpc `GracefulStop`/drain but
  **before** `cachekv.Shutdown(...)`: block on a forced save —
  `resp := make(chan string, 1); cmdChannel <- CkvCommand{Op: "SAVE", Args: []string{"force"}, Resp: resp}; <-resp`.
  This ordering is mandatory per the `cachekv.Shutdown()` note above.

### 5. New flags in `main()`

- `--persist-db` (string, default `"cachestore"`) — backing database name.
- `--save-interval` (duration, default `60s`) — ticker period.
- `--save-min-changes` (int, default `1`) — dirty-count threshold per tick,
  mirroring Redis's `save <sec> <changes>` directives (one rule for v1).

## Known gaps (called out, not silently dropped)

- **Deletions aren't mirrored.** `BatchInsert` only upserts. Currently moot in
  practice: `commandRegistry` has no `DEL`/`HDEL`/`SREM` yet, so nothing can
  actually remove a key from `Datastore` today. Once a delete command exists,
  proper mirroring needs diffing the previously-persisted keyset against the
  current one and calling `RemoveEntry` for what disappeared — proposed as a
  follow-up stage, not part of this pass.
- **Cross-type key collisions.** `Datastore` already allows the same name to
  exist independently in `Strings`, `Hashes`, and `Sets` (unlike Redis's
  single keyspace). This plan just persists whatever's there under distinct
  prefixes; it doesn't change that pre-existing in-memory behavior.
- **`SAVE` blocks the single processor goroutine** for the duration of the
  `BatchInsert` call — like Redis's synchronous `SAVE`, not `BGSAVE`/fork. No
  second concurrency layer for now; revisit only if save latency becomes
  visible to connected clients.

## Sequencing

1. **Persistence primitives, forced save/load only (no ticker).**
   `persistence.go` (`snapshot`/`saveToDisk`/`loadFromDisk`), `stateProcessor`
   takes an initial `*Datastore` and handles `Op == "SAVE"`, `main()`
   creates/loads the persist db at startup and forces one save in the
   shutdown path. **Verify:** set keys over the raw listener, `SIGTERM`,
   restart the server, confirm the keys survived.
2. **Ticker + dirty counter.** `runPeriodicSave`, `--save-interval` /
   `--save-min-changes` flags. **Verify:** writes only hit disk on a tick
   with `dirty > 0`; an idle server produces no repeated disk writes (check
   via `cachekv`'s own log lines or the store dir's mtimes).
3. **Follow-up (separate pass):** deletion mirroring via keyset diffing once
   a delete command exists; optionally expose `SAVE`/`BGSAVE` as real
   client-facing commands in `commandRegistry`, matching Redis's own surface.

## Verification

- `go build ./...` / `go vet ./...` clean at each stage.
- Run `cmd/cachekv-server --run-as-cachestore=true --store-path=<tmp>
  --key-path=<tmp> --save-interval=2s --save-min-changes=1`, `SET`/`HSET`/`SADD`
  a few keys via `nc` against `--cachestore-listen`, wait past a tick, restart
  the process, confirm the values reappear (via `GET`/`HGET` after restart).
- `SIGTERM` immediately after a write (before the next tick fires) and confirm
  the shutdown-triggered forced save still persisted it.
