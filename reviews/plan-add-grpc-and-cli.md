# Add gRPC server + CLI client to cachekv

## Context

`cachekv` is currently a pure Go library (package `cachekv/`) wrapping BadgerDB with
encryption, a keyring, a meta database, and a connection pool. The only executable
today is `cachekv.go` (root, `package main`) — a demo binary that just calls
`cachekv.Startup()`, waits for a signal, and calls `cachekv.Shutdown()`. There is no
network-facing server and no CLI. The user wants to expose the library's operations
(create/insert/update/remove/get/list/config/metrics) over gRPC to remote clients,
and a CLI that talks to that server.

Correction to note: `vendor/` and `go.sum` are currently **untracked** (`git status`
shows `?? go.sum` / `?? vendor/`) — they were never committed. So this is not a
"vendored repo" in practice; adding `google.golang.org/grpc` is a normal
`go mod tidy` with no special vendor-diff concern.

Toolchain check: `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` are already on PATH.
`google.golang.org/protobuf` is already a dependency (used only for Badger's internal
`pb.KV` marshalling in `All()`, unrelated to gRPC). No `google.golang.org/grpc` dep
exists yet.

## Decisions (called out, not re-litigated per-stage)

- **No TLS/auth in this pass.** Ships as plaintext, unauthenticated gRPC — fine for a
  trusted network/localhost use case. Flagged clearly in README as a known
  limitation. Auth/TLS is a distinct future workstream.
- **`GetAll` stays server-streaming**, but reuses `(*Storage).All()` as-is (which
  already buffers into a `map[string][]byte` internally) — server ranges over the
  map and sends one `Entry` per stream message. This avoids exceeding gRPC's 4MB
  default message cap on large DBs and lets clients consume incrementally, without
  requiring changes to the library's internals.
- **`BatchInsert` stays unary** (`repeated Entry` in one request), matching the
  library's existing all-or-nothing batch semantics — no incremental server commit
  exists to justify client-streaming.
- **Generated proto code is checked into the repo** (not generated at build time),
  since there's no existing codegen-on-build infra and this keeps `go build ./...`
  zero-dependency for consumers.
- **CLI uses stdlib `flag` with hand-rolled subcommand dispatch**, not a framework
  like cobra — the command surface is small (~12 subcommands, no nesting), and the
  repo has zero CLI-framework dependencies today.
- **Root `cachekv.go` is left alone** in this pass; `cmd/cachekv-server` is additive.
  Decide its fate later once the new binary is proven out.
- Error mapping from library errors to gRPC status codes will initially rely on
  `errors.As` for the two typed errors (`ValidationError`, `EMetaKeyNotFound`) plus
  substring matching for the library's unexported plain-string errors (e.g.
  "already exists", "rotating key", "inactive db", "shutting down"). This is a
  little brittle but avoids touching `cachekv/db.go`'s error handling as a
  prerequisite — acceptable for now; can be revisited if it causes real friction.

## Proto / Service Design

New file `proto/cachekv/v1/cachekv.proto`, package `cachekv.v1`, `go_package`
pointing at `github.com/fluxodesign/cachekv/gen/cachekv/v1`. Service `CacheKV`:

| RPC | Type | Maps to |
|---|---|---|
| `CreateDatabase` | unary | `CreateDatabase(dbName, secure)` |
| `InsertEntry` | unary | `InsertEntry(dbName, key, value)` |
| `UpdateEntry` | unary | `UpdateEntry(dbName, key, value)` |
| `RemoveEntry` | unary | `RemoveEntry(dbName, key)` |
| `BatchInsert` | unary (repeated Entry) | `BatchInsert(dbName, entries)` |
| `GetEntry` | unary | `GetEntry(dbName, key)` |
| `GetAll` | server-streaming | `GetStorageObject(dbName)` + `store.All()` |
| `ListDatabases` | unary | `ListDatabases()` |
| `ListConfigurations` | unary | `ListConfigurations()` |
| `UpdateConfigurations` | unary | `UpdateConfigurations(config)` |
| `GetMetrics` | unary | `GetMetricsCollector().(*SimpleMetricsCollector).GetMetrics()` |
| `Ping` | unary | trivial liveness |

Also register the standard `grpc_health_v1.Health` service (readiness — flips to
`NOT_SERVING` when `cachekv.IsShuttingDown()`/on shutdown signal).

Messages are simple structs mirroring the Go types (`Entry{key, value}`,
`ConfigMessage` mirroring `cachekv.Config`, `GetMetricsResponse` mirroring
`cachekv.Metrics` with `google.protobuf.Duration` fields for latencies).

Error mapping lives in `internal/grpcserver/errors.go` (`toStatus(err error) error`):
`ValidationError` → `InvalidArgument`, `EMetaKeyNotFound`/`badger.ErrKeyNotFound` →
`NotFound`, shutdown-related → `Unavailable`, "already exists" → `AlreadyExists`,
"inactive db" → `FailedPrecondition`, everything else → `Internal`.

## Server Wiring

- **`cmd/cachekv-server/main.go`**: flags/env for `--listen` (default `:50051`),
  `--store-path`, `--key-path` (set `cachekv.StorePath`/`KeyPath` before `Startup()`).
  Calls `cachekv.Startup()`, builds `grpc.NewServer()`, registers `CacheKV` +
  `Health` services, serves on a `net.Listener`, waits for SIGTERM/SIGINT.
- **Shutdown order matters**: on signal, (1) flip health to `NOT_SERVING`, (2)
  `grpcServer.GracefulStop()` (bounded by a timeout with `Stop()` fallback so it
  can't hang forever), (3) only then call `cachekv.Shutdown(ctx, timeout)`. Doing it
  in the reverse order would let in-flight RPCs start failing mid-stream against a
  closing connection pool. Reuse the existing metrics-report printing from today's
  `cachekv.go` after shutdown completes.
- **`internal/grpcserver/server.go`**: `Server` type embedding
  `cachekvv1.UnimplementedCacheKVServer`; each method is a thin adapter (unmarshal →
  call `cachekv` function → `toStatus` → marshal).
- **`internal/grpcserver/health.go`**: wraps `grpc/health`, toggled from the
  server's shutdown sequence.

## Codegen Workflow

New root `Makefile` target:
```makefile
proto:
	protoc --proto_path=proto \
	  --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
	  proto/cachekv/v1/cachekv.proto
```
Generated code lands in `gen/cachekv/v1/*.pb.go`, checked in. After first adding the
grpc import: `go mod tidy` (go.sum updates; vendor/ is untracked so no vendor step
needed unless the user wants vendoring going forward — not assumed here).

## CLI Design

New `cmd/cachekv-cli/`:
- `main.go` — global flags parsed before the subcommand (`--addr`/`CACHEKV_ADDR`,
  default `localhost:50051`, insecure dial; `--json` for structured output), then
  dispatch on `os.Args[1]`.
- `client.go` — `dial(addr string) (cachekvv1.CacheKVClient, io.Closer, error)`.
- `cmd_db.go`, `cmd_entry.go`, `cmd_config.go`, `cmd_admin.go` — one file per command
  group.
- Command surface: `db create <name> [--secure]`, `db list`, `put <db> <key> <value>`,
  `update <db> <key> <value>`, `rm <db> <key>`, `batch-insert <db> --file entries.json`,
  `get <db> <key>`, `all <db>` (prints as it streams), `config get`,
  `config set <json-file>`, `metrics`, `ping`.
- Output: plain text by default, `--json` marshals hand-mapped CLI-facing structs
  (not raw protojson, to keep field naming clean for scripting).

## Testing / Verification

- `make proto` + `go build ./gen/...` as the first gate.
- `internal/grpcserver/server_test.go` using `google.golang.org/grpc/test/bufconn`:
  start `cachekv.Startup()` against a `t.TempDir()`-backed store, register the real
  `Server` on an in-memory listener, exercise every RPC end-to-end (create → put →
  get → matches; batch-insert → get-all; remove → get → NotFound; list-dbs; config
  round-trip; metrics non-nil after ops; health returns SERVING). Note: `cachekv`'s
  global state means these tests can't trivially run in parallel with each other —
  keep them sequential within the package.
- `internal/grpcserver/errors_test.go` — table test asserting `toStatus` codes.
- CLI logic tested against the same `bufconn` harness where possible; one real
  end-to-end smoke test builds the binary and runs it against a real
  `127.0.0.1:0` listener to cover `main()`/flag-parsing wiring.
- Manual check per stage: start `cmd/cachekv-server`, hit health/ping with
  `grpcurl` or the CLI, round-trip a put/get, run through the full CLI command list.

## Sequencing (small, reviewable steps)

1. **Proto + codegen + server skeleton**: `.proto` file (full RPC surface defined),
   `Makefile`, generated `gen/` code, `go.mod`/`go.sum` updated, `internal/grpcserver`
   with only `Ping` + health wired (other RPCs return `Unimplemented` for free via
   the embedded type), `cmd/cachekv-server/main.go` with full startup/shutdown
   wiring. Verify: server starts, health check passes, graceful shutdown ordering
   correct.
2. **Wire real RPCs**: implement all remaining `CacheKV` methods, `errors.go` +
   its test, full `bufconn` integration suite. Can split into read-only RPCs then
   write RPCs as two smaller PRs if preferred.
3. **CLI skeleton + read-only commands**: `main.go`, `client.go`, global flags,
   `db list`, `get`, `all`, `config get`, `ping`, `metrics`.
4. **Remaining CLI write commands + polish**: `db create`, `put`, `update`, `rm`,
   `batch-insert`, `config set`, `--json` flag everywhere, README updates covering
   server/CLI usage, the no-TLS/no-auth limitation, and the `make proto` workflow.

## Critical files

- `cachekv/db.go`, `cachekv/types.go`, `cachekv/metrics.go`, `cachekv/pool.go` — the
  API being wrapped.
- `cachekv.go` (root) — existing demo binary/shutdown pattern to mirror.
- `go.mod` — add `google.golang.org/grpc`.
- New: `proto/cachekv/v1/cachekv.proto`, `Makefile`, `gen/cachekv/v1/`,
  `internal/grpcserver/`, `cmd/cachekv-server/`, `cmd/cachekv-cli/`.
