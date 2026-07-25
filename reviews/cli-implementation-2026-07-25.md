I'll check the plan file first.

Now let me verify the current state of the repo.

`internal/grpcserver/errors.go` and the tests are missing. Let me check what's actually implemented in the server.

Server Wiring is done, but note the gap: only `Ping` is implemented on the server — stage 2 ("Wire real RPCs" + `errors.go`) hasn't been done, so CLI commands other than `ping` will get `Unimplemented` until that lands. The CLI itself only depends on the generated client stubs, which exist, so I can build it now. Flagging it so you can decide on ordering.

Let me read the existing code to match style and get the exact message shapes.

Both confirmed nil-safe/available. Now writing the CLI.

Now the entry commands:

Builds clean. Now a live smoke test against the real server:

Server is up. Exercising the CLI:

Ping works end-to-end; everything else correctly reports `Unimplemented` (server stage 2). Now the local input-validation paths:

One rough edge: `parsing -:` should read as `stdin`. Fixing that:

Only `assert` is vendored and the repo's existing tests use `assert` exclusively — matching that instead of re-vendoring:

Now the README — the `batch-insert` file format and `--json` behavior aren't discoverable otherwise:

Now a final full verification, and shutting down the test server:

CLI implemented, building clean, `go vet`/`gofmt` clean, tests passing, and verified against a live server.

**Files added** (`cmd/cachekv-cli/`, matching the plan's layout):
- `main.go:74` — global flags (`--addr`/`CACHEKV_ADDR`, `--json`, `--timeout`), dispatch table, usage output, exit codes (0/1/2), gRPC-status error formatting
- `client.go` — insecure `dial()` via `grpc.NewClient`
- `output.go` — hand-mapped JSON result structs, text/JSON emit helpers, UTF-8-vs-base64 value rendering, file/stdin input decoding
- `cmd_db.go`, `cmd_entry.go`, `cmd_config.go`, `cmd_admin.go` — the full 12-command surface
- `cli_test.go` — tests for the pure logic (flag parsing, entries-file parsing, config patching, value rendering, dispatch)

Plus a README section covering server/CLI usage, the no-TLS/no-auth limitation, `make proto`, and the `batch-insert` file format.

**Three judgment calls worth your review:**

1. **`config set` patches rather than replaces.** `UpdateConfigurations` overwrites the whole config, so a partial file like `{"secure_new_db":true}` would blank `store_path`. The CLI reads the current config and overlays only the keys present in the file (pointer fields detect presence). Deviates slightly from the plan's plain `config set <json-file>`, but avoids a footgun that destroys server config.
2. **Flags may follow positionals.** `db create mydb --secure` reads better than the reverse, but stdlib `flag` stops at the first non-flag arg — `parseFlagsAnywhere` (`main.go:180`) loops `Parse` to handle both orders.
3. **`all --json` emits NDJSON**, one object per line, since a single array can't be printed until the whole stream is buffered.

**Two things you should know:**

- **Only `ping` actually works.** Stage 2 ("Wire real RPCs" + `internal/grpcserver/errors.go`) isn't done — `server.go` still implements just `Ping`, so every other CLI command returns `Unimplemented: method X not implemented` and exits 1. The CLI plumbing is verified; its behavior against real data isn't, and can't be until stage 2 lands.
- **Tests cover pure logic only.** The plan's `bufconn` harness for CLI commands needs a real server implementation to assert against, so that belongs with stage 2. I also used `assert` rather than `require` — `require` isn't in `vendor/` and the existing `cachekv/*_test.go` files use `assert` exclusively; adding `require` would have meant re-running `go mod vendor`.

Want me to do stage 2 next so the CLI is actually exercisable end-to-end?