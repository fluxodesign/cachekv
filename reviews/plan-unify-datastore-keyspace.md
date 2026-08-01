# Unify the cache store's keyspace

## Context

`cmd/cachekv-server`'s cache-store mode keeps three independent Go maps
(`datastore.go:61-65`):

```go
type Datastore struct {
	Strings map[string]string
	Hashes  map[string]map[string]string
	Sets    map[string]map[string]struct{}
}
```

Redis has **one** keyspace: a key holds exactly one value of exactly one type,
and a type-specific command against the wrong type replies
`-WRONGTYPE Operation against a key holding the wrong kind of value`. Here,
`SET foo bar`, `HSET foo f v` and `SADD foo m` all succeed against the same
name and produce three unrelated values that coexist forever. This was already
flagged as a known gap in `plan-add-disk-persistence-for-in-memory-cache-store.md:110-113`
("Cross-type key collisions"), deliberately left alone by that pass.

It now blocks real work. `DEL`, `EXISTS`, `TYPE`, `KEYS`, `RENAME`, `COPY`,
`EXPIRE` and every future type (lists, sorted sets) are all defined over a
single keyspace — none of them have a coherent meaning while a name can denote
three things at once. Unifying is the prerequisite for the whole rest of the
command surface.

The unification is not purely an in-memory refactor. It creates two situations
where a key's *persisted* name is orphaned, and `saveToDisk` is a
`BatchInsert`, which only upserts:

1. **Existing on-disk collisions.** A store that already persisted `s:foo`,
   `h:foo` and `z:foo` must collapse to one on load. The losers stay on disk
   forever, re-triggering the same collision on every restart.
2. **Runtime type replacement.** Redis's `SET` overwrites a key *regardless of
   its current type* (only type-specific read/modify commands return
   WRONGTYPE). So `HSET foo f v` then `SET foo bar` legitimately turns a hash
   into a string, orphaning `h:foo` on disk. Next restart resurrects it as a
   collision.

So this pass has to carry the minimal delete-mirroring that the persistence
plan deferred (`...disk-persistence...md:104-109`). The good news: unification
makes it *precise* rather than expensive. Because a key has one type, we know
the exact orphaned disk key at the moment it's orphaned — no keyset diffing
required.

## Key files

- `cmd/cachekv-server/datastore.go` — `Datastore`, the six handlers,
  `commandRegistry` (`:78-85`), `stateProcessor` (`:22`). All mutation happens
  on the single `stateProcessor` goroutine; nothing here needs locks and
  nothing added here may be read from another goroutine.
- `cmd/cachekv-server/persistence.go` — `snapshot` (`:20`), `saveToDisk`
  (`:45`), `loadFromDisk` (`:63`), and the `s:`/`h:`/`z:` prefixes (`:11-15`).
- `cmd/cachekv-server/main.go` — the `if *runAsCachestore` block that loads the
  store and starts `stateProcessor`; new flag goes here.
- `cachekv/db.go` — `GetStorageObject` (`:512`) returns a `*Storage` holding a
  pool reference; `(*Storage).RemoveEntry` (`:1079`) and
  `(*Storage).BatchInsert` (`:1126`) reuse that one handle. Prefer these over
  the package-level `RemoveEntry` (`:1032`) / `BatchInsert` (`:1090`), which
  re-resolve the meta object and take a fresh pool reference **per call** — a
  per-key `RemoveEntry` loop through the package-level function would be one
  full pool acquire/release per deleted key. The package-level `RemoveEntry`
  also rejects operations once `IsShuttingDown()` is set, which the shutdown
  forced-save path should not have to reason about; the `*Storage` method
  doesn't.

Nothing outside `package main` touches `Datastore`, and there is no
`datastore_test.go` today, so the struct and its field names are free to change
without ripple.

## Decisions (called out, not re-litigated per-stage)

- **Tagged struct, not `any` or an interface.** Three types today, maybe six
  later; a `kind` tag plus one populated payload field keeps every handler free
  of type assertions and keeps `snapshot`'s switch exhaustive-checkable by eye.
  An `any` payload buys nothing but assertion noise at ~20 call sites.
- **On-disk format does not change.** The `s:`/`h:`/`z:` prefixes already
  encode exactly one type per key; under a unified keyspace they simply stop
  being able to collide legitimately. Load-side reconciliation changes, the
  encoding doesn't. No migration, no dual-read path.
- **`z:` for *sets* stays**, despite `z` conventionally meaning sorted set in
  Redis. Renaming it means a migration for a cosmetic win. Sorted sets, when
  they arrive, take a different letter — reserve `l:` lists, `o:` sorted sets
  (ordered), and write the table into `persistence.go` so the next person
  doesn't reach for `z:`.
- **Collision precedence is fixed and arbitrary: string > hash > set.** There
  is no timestamp in a flat KV store to pick a "newer" value, so the rule just
  has to be *deterministic* — a naive "first one wins" would depend on
  `(*Storage).All()`'s map iteration order and pick a different winner on each
  restart. That is the failure mode to avoid; which letter wins matters far
  less.
- **`SET` overwrites any type, matching Redis.** The alternative (reject `SET`
  against a non-string) is one line cheaper and wrong, and would have to be
  un-picked the first time a client relies on Redis semantics.
- **Scope excludes `DEL`/`HDEL`/`SREM` and expiry.** This pass builds the
  machinery that makes them small; it does not add them.

## Design

### 1. The value model

New in `datastore.go`:

```go
type valueKind uint8

const (
	kindString valueKind = iota
	kindHash
	kindSet
)

// String is the reply for the TYPE command and the wording used in
// collision logs.
func (k valueKind) String() string   // "string" | "hash" | "set"
// prefix is the persisted-key prefix for this kind (see persistence.go).
func (k valueKind) prefix() string   // "s:" | "h:" | "z:"

// value is one entry in the keyspace: a kind tag plus exactly one populated
// payload, chosen by that tag. The other payloads are always nil/zero.
type value struct {
	kind valueKind
	str  string
	hash map[string]string
	set  map[string]struct{}
}

type Datastore struct {
	keys map[string]*value

	// stale holds persisted keys (already prefixed) whose in-memory value is
	// gone because the key changed type. saveToDisk removes them, since
	// BatchInsert only upserts. Not part of snapshot.
	stale map[string]struct{}
}
```

`keys`/`stale` are unexported: everything that touches them lives in
`package main`, and unexported makes it obvious that the accessors below are
the intended door.

### 2. Accessors that centralise WRONGTYPE

Every handler goes through one of two helpers, so the WRONGTYPE rule is written
once rather than six times:

```go
// lookup returns the value at key when it holds kind. found is false when the
// key is absent. errReply is a non-empty RESP error when the key exists with a
// different kind, in which case the caller returns it verbatim.
func (d *Datastore) lookup(key string, kind valueKind) (v *value, found bool, errReply string)

// mutable returns the value at key, creating an empty one of kind if absent.
// errReply is non-empty (WRONGTYPE) when the key exists with a different kind,
// in which case nothing is created.
func (d *Datastore) mutable(key string, kind valueKind) (v *value, errReply string)

// replace stores v at key whatever was there before, recording the previous
// kind's persisted key as stale when the kind changed. This is SET's
// type-agnostic overwrite.
func (d *Datastore) replace(key string, v *value)
```

`replace` is the only place that writes to `stale`, and it does so only when
`old.kind != v.kind`.

Handlers become, e.g.:

```go
func hgetCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("hget")
	}
	v, found, errReply := store.lookup(args[0], kindHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	val, ok := v.hash[args[1]]
	if !ok {
		return nilBulk
	}
	return bulkString(val)
}
```

### 3. Reply helpers

The handlers currently hand-roll RESP with `fmt.Sprintf` at every return. With
WRONGTYPE added to the mix it's worth a small set of shared constructors in
`datastore.go` (or a `reply.go`):

```go
const (
	okReply      = "+OK\r\n"
	pongReply    = "+PONG\r\n"
	nilBulk      = "$-1\r\n"
	errWrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
)

func bulkString(s string) string  // $<len>\r\n<s>\r\n
func integer(n int) string        // :<n>\r\n
func wrongArgs(cmd string) string // -ERR wrong number of arguments for '<cmd>' command\r\n
```

Note the prefix: Redis replies `-WRONGTYPE`, not `-ERR WRONGTYPE`. Clients
match on that token.

Purely mechanical, but it keeps the rewritten handlers readable and stops the
WRONGTYPE string from being retyped with a typo.

### 4. Persistence: snapshot

`snapshot` switches on `v.kind` instead of walking three maps. Same output
bytes, same prefixes.

### 5. Persistence: load-side reconciliation

Split the current `loadFromDisk` into I/O plus a **pure** function, mirroring
how `snapshot` is already pure — this is what makes the collision logic
testable without standing up a real `cachekv` database:

```go
// reconcile demultiplexes persisted entries into a unified keyspace. When a
// logical key was persisted under more than one prefix, the highest-precedence
// kind (string > hash > set) wins and the losing prefixed keys are returned in
// sorted order for purging. Unparseable and unknown-prefix entries are
// reported as skipped rather than dropped silently.
func reconcile(entries map[string][]byte) (store *Datastore, losers []string, skipped []string)
```

`loadFromDisk` then: `GetStorageObject` → `All()` → `reconcile` → act on
`losers` per the collision policy below → return the store.

Precedence must be evaluated over *all* entries for a key before choosing, not
as a running "first wins" — `All()` returns a map, and its iteration order is
randomised per run.

### 6. Collision policy: `--on-key-collision`

New flag in `main.go`, `warn` (default) or `fail`:

- **`warn`** — log one line per collision naming the key, the winning kind, and
  each dropped kind with its payload size; then purge the losing prefixed keys
  from disk via `(*Storage).RemoveEntry` while the load handle is still open,
  and continue booting. Purging is the point: without it the dropped data
  lingers and the same warning fires on every restart forever.
- **`fail`** — log the same lines and `log.Fatalf` without touching disk, so an
  operator can inspect or export the data (via the gRPC/CLI surface, which
  reads the same database) before deciding.

Default `warn` because this is a cache store: refusing to boot converts a data
oddity into an outage, and the data is by definition reconstructible. `fail`
exists for anyone who'd rather not have startup delete anything. The trade-off
is real and the flag is the honest way to expose it — `warn` *does* destroy the
losing values.

### 7. Persistence: saveToDisk with deletes

```go
func saveToDisk(dbName string, store *Datastore) error {
	entries := snapshot(store)
	if len(store.stale) == 0 {
		return cachekv.BatchInsert(dbName, entries) // unchanged fast path
	}

	storage, err := cachekv.GetStorageObject(dbName)
	if err != nil {
		return err
	}
	defer storage.Close()

	// Deletes first: a key can be orphaned and then re-created under the same
	// prefixed name, and the insert must win.
	for k := range store.stale {
		if err := storage.RemoveEntry(k); err != nil {
			return err // leave the rest of stale intact for the next save
		}
		delete(store.stale, k)
	}
	return storage.BatchInsert(&entries)
}
```

Two ordering hazards worth stating, both of which the code above handles:

- **A prefixed key can be both stale and live.** `SET foo a` (persists `s:foo`)
  → `DEL foo` (marks `s:foo` stale) → `SET foo b` (`s:foo` live again). Running
  deletes before inserts keeps the value. Belt and braces: `replace` and
  `mutable` should `delete(d.stale, key.prefix()+key)` whenever they write a
  key, so the entry never lingers in both sets. (`DEL` doesn't exist yet — this
  is the sequence the mechanism must already survive when it lands.)
- **A failed save must not lose the delete.** `stateProcessor` only resets
  `dirty` on success (`datastore.go:29-31`); `stale` must behave the same way,
  which is why entries are removed one at a time as they succeed rather than
  cleared at the end.

### 8. `TYPE` (optional, one handler)

`TYPE key` → `+string`/`+hash`/`+set`, or `+none` when absent. Three lines on
top of `valueKind.String()`, and it's the black-box way to verify unification
over the wire without reaching into process memory. Dropped from the pass at no
cost if the scope line is worth holding.

## Known gaps (called out, not silently dropped)

- **`warn` mode deletes data on startup.** By design, logged per key, but it's
  a destructive default. `fail` is one flag away.
- **Still no `DEL`/`HDEL`/`SREM`.** The `stale` mechanism is exactly what they
  need — `DEL` becomes "remove from `keys`, add the prefixed name to `stale`" —
  but they're not in this pass and the mechanism ships only exercised by
  `SET`-over-another-type.
- **No expiry**, so no interaction with TTL semantics to worry about yet. When
  `EXPIRE` lands, a lazily-expired key is another `stale` producer.
- **`stale` is unbounded between saves** in the pathological case (type-flip
  every key). Bounded by the number of distinct keys, i.e. by the store itself,
  so it can't outgrow what's already in memory.
- **`SAVE` remains client-reachable and unauthenticated** (`datastore.go:26`,
  ahead of the registry lookup) and now also performs deletes. Unrelated to
  this change, but the blast radius of that gap grows slightly. Separate
  workstream.
- **Sorted sets will need a prefix that isn't `z:`.** Reserved above; the debt
  is a comment, not code.

## Sequencing

1. **Value model + handlers.** `valueKind`/`value`/unified `Datastore`,
   `lookup`/`mutable`/`replace`, reply helpers, all six handlers rewritten,
   `snapshot` switched over, `reconcile` with precedence (no flag, no purge —
   losers are logged and dropped in memory only). Server is unified in memory
   and still saves/loads.
   **Verify:** `SET foo bar` then `HSET foo f v` → WRONGTYPE; `HSET h f v` then
   `GET h` → WRONGTYPE; `SET h x` succeeds and `HGET h f` then → WRONGTYPE.
2. **Collision policy.** `--on-key-collision`, per-key logging, purge of losers
   in `warn` mode.
   **Verify:** hand-build a colliding store (run the *current* binary, `SET x
   v` / `HSET x f v` / `SADD x m`, SIGTERM), then boot the new binary — one
   warning per dropped kind, `TYPE x` (or `GET x`) shows the winner, and a
   second restart is silent because the losers are gone from disk. Same store
   with `--on-key-collision=fail` refuses to boot and leaves the database
   byte-identical.
3. **Delete mirroring in `saveToDisk`.** `stale` tracking in `replace`,
   `*Storage`-based save path, one-at-a-time drain.
   **Verify:** `HSET k f v`, wait for a save tick, `SET k s`, wait for another
   tick, restart — `TYPE k` is `string`, no collision warning, and the run
   after that is silent too.
4. **Optional: `TYPE`.**

## Verification

- `go build ./...` / `go vet ./...` clean at each stage. Note `vendor/` is
  currently stale against `go.mod` (`go mod vendor` is due, unrelated to this
  work); until it's refreshed these need `-mod=mod`.
- New `cmd/cachekv-server/datastore_test.go`:
  - WRONGTYPE matrix — each of `GET`/`HSET`/`HGET`/`SADD` against a key holding
    each of the other two kinds, plus the absent-key case.
  - `SET` overwrites a hash and a set, and records the right prefixed key in
    `stale`.
  - `replace`/`mutable` clear a matching entry from `stale`.
- New tests for `reconcile` (pure, no database needed):
  - Three-way collision resolves to `string`, losers are `["h:x", "z:x"]`.
  - **Determinism:** run the same input map through `reconcile` repeatedly and
    assert an identical winner and loser list every time. This is the
    regression test for the map-iteration-order trap in §5.
  - Unknown prefixes and undecodable JSON land in `skipped`, not dropped.
- Manual round-trip per the sequencing steps above, against
  `--save-interval=2s --save-min-changes=1` and a temp `--store-path`/
  `--key-path`, driven over the raw listener.
