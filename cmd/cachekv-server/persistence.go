package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fluxodesign/cachekv/cachekv"
)

const (
	stringPrefix = "s:"
	hashPrefix   = "h:"
	setPrefix    = "z:"
	expirePrefix = "e:"
	listPrefix   = "l:"
	zsetPrefix   = "o:"

	// z: stays on sets despite the naming clash with sorted sets — see
	// plan-unify-datastore-keyspace.md. o: ("ordered") is sorted sets'
	// prefix instead, reserved for exactly this by that same plan.
)

// Values for the --on-key-collision flag (main.go), governing what
// loadFromDisk does when reconcile finds a logical key persisted under more
// than one type prefix.
const (
	collisionPolicyWarn = "warn"
	collisionPolicyFail = "fail"
)

// typeFromPrefix is reconcile's precedence encoding read backwards, for
// turning a losing prefixed key back into a human-readable type name in logs.
func typeFromPrefix(prefix string) valueType {
	switch prefix {
	case hashPrefix:
		return typeHash
	case setPrefix:
		return typeSet
	case listPrefix:
		return typeList
	case zsetPrefix:
		return typeZSet
	default:
		return typeString
	}
}

// snapshot flattens store's unified keyspace into a single key->value set
// suitable for cachekv.BatchInsert, prefixing each key by its logical type so
// they can share one namespace without colliding. A key carrying a TTL gets
// one extra "e:"-prefixed entry alongside its value entry.
func snapshot(store *Datastore) map[string][]byte {
	entries := make(map[string][]byte, len(store.keys))

	for key, v := range store.keys {
		switch v.kind {
		case typeString:
			entries[stringPrefix+key] = []byte(v.str)
		case typeHash:
			if b, err := json.Marshal(v.hash); err == nil {
				entries[hashPrefix+key] = b
			}
		case typeSet:
			names := make([]string, 0, len(v.set))
			for member := range v.set {
				names = append(names, member)
			}
			if b, err := json.Marshal(names); err == nil {
				entries[setPrefix+key] = b
			}
		case typeList:
			if b, err := json.Marshal(v.list); err == nil {
				entries[listPrefix+key] = b
			}
		case typeZSet:
			if b, err := json.Marshal(v.zset); err == nil {
				entries[zsetPrefix+key] = b
			}
		}
		if v.expireAt != 0 {
			entries[expirePrefix+key] = []byte(strconv.FormatInt(v.expireAt, 10))
		}
	}
	return entries
}

// saveToDisk persists store's current contents to the named cachekv database
// in a single batch, mirroring one Redis-style RDB snapshot. When a key was
// deleted or changed type since the last save, the prefixed name it no longer
// occupies is removed first — BatchInsert only upserts, so an orphaned
// prefixed key would otherwise linger on disk forever and come back as a
// resurrected value (or a collision) on the next restart.
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
	// prefixed name, and the insert must win. One batch rather than a
	// RemoveEntry per key, because DEL/HDEL/SREM can orphan an unbounded
	// number of names between two save ticks.
	//
	// stale is cleared only on success, mirroring how stateProcessor only
	// resets dirty on a successful save. RemoveEntries is not atomic, so an
	// error may leave some of these already gone from disk; deletes are
	// idempotent, so re-sending the whole set on the next save is harmless.
	orphaned := make([]string, 0, len(store.stale))
	for k := range store.stale {
		orphaned = append(orphaned, k)
	}
	if err := storage.RemoveEntries(orphaned); err != nil {
		return err
	}
	clear(store.stale)

	return storage.BatchInsert(&entries)
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

// runActiveExpire sends an internal "EXPIRECYCLE" pseudo-command every
// interval, mirroring runPeriodicSave. Passive (access-time) expiration alone
// would leave a TTL'd key nobody ever touches again sitting in memory (and
// re-persisted on every save) forever.
func runActiveExpire(interval time.Duration, cmdChannel chan<- CkvCommand) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		cmdChannel <- CkvCommand{Op: "EXPIRECYCLE"}
	}
}

// loadFromDisk reconstructs a *Datastore from whatever was last persisted to
// the named cachekv database, reversing the prefixing done by snapshot and
// resolving any cross-type collisions left over from before unification per
// onCollision (collisionPolicyWarn or collisionPolicyFail).
//
// warn logs one line per collision, purges the losing prefixed keys from
// disk while the load handle is still open (so the same warning doesn't fire
// again on the next restart), and boots with the winners. fail logs the same
// lines and calls log.Fatalf without touching disk, leaving the database
// byte-identical for inspection.
func loadFromDisk(dbName string, onCollision string) (*Datastore, error) {
	storage, err := cachekv.GetStorageObject(dbName)
	if err != nil {
		return nil, err
	}
	defer storage.Close()

	all, err := storage.All()
	if err != nil {
		return nil, err
	}

	store, losers, skipped, orphanedExpiry := reconcile(all)
	for _, key := range skipped {
		log.Printf("cache store: skipping unparseable or unknown-prefix persisted key %q", key)
	}

	if len(losers) > 0 {
		for _, line := range describeCollisions(store, all, losers) {
			log.Println(line)
		}
		if onCollision == collisionPolicyFail {
			log.Fatalf("cache store: refusing to start with --on-key-collision=fail; resolve the collisions above and restart")
		}
		for _, loser := range losers {
			if err := storage.RemoveEntry(loser); err != nil {
				log.Printf("cache store: failed to purge colliding key %q: %v", loser, err)
			}
		}
	}

	// Orphaned expiry entries destroy nothing but an already-meaningless
	// timestamp for a key that's already gone — always safe to purge, unlike
	// a type-precedence loser, so this isn't gated behind --on-key-collision.
	if len(orphanedExpiry) > 0 {
		if err := storage.RemoveEntries(orphanedExpiry); err != nil {
			log.Printf("cache store: failed to purge orphaned expiry entries: %v", err)
		}
	}

	return store, nil
}

// describeCollisions renders one log line per colliding logical key, naming
// the winning kind and every dropped kind with its persisted payload size.
// Pure (no I/O, no logging) so the collision policy's messaging is testable
// without a database.
func describeCollisions(store *Datastore, all map[string][]byte, losers []string) []string {
	byName := make(map[string][]string)
	for _, loser := range losers {
		name := loser[2:] // every prefix (s:/h:/z:) is exactly 2 bytes
		byName[name] = append(byName[name], loser)
	}

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)

	lines := make([]string, 0, len(names))
	for _, name := range names {
		dropped := make([]string, 0, len(byName[name]))
		for _, loser := range byName[name] {
			kind := typeFromPrefix(loser[:2])
			dropped = append(dropped, fmt.Sprintf("%s (%d bytes)", kind, len(all[loser])))
		}
		lines = append(lines, fmt.Sprintf("cache store: key collision on %q: keeping %s, dropping %s",
			name, store.keys[name].kind, strings.Join(dropped, ", ")))
	}
	return lines
}

// reconcile demultiplexes persisted entries into a unified keyspace. When a
// logical key was persisted under more than one prefix, the
// highest-precedence kind (string > hash > set) wins and the losing prefixed
// keys are returned in sorted order for purging. Unparseable and
// unknown-prefix entries are reported as skipped rather than dropped
// silently.
//
// "e:"-prefixed entries carry TTLs and are applied to their winning key
// separately from the type-precedence logic above — a key has at most one
// TTL regardless of kind. One with no matching live key (stale data left
// over from a deleted key, not a type collision) comes back as
// orphanedExpiry rather than losers: losers feeds describeCollisions, which
// assumes every name has a live winner, and unlike a type collision this
// doesn't drop any real data or need an operator's --on-key-collision call.
func reconcile(entries map[string][]byte) (store *Datastore, losers []string, skipped []string, orphanedExpiry []string) {
	store = NewDatastore()

	type candidate struct {
		kind valueType
		v    *value
	}
	candidatesByName := make(map[string][]candidate)
	expireByName := make(map[string]int64)

	for key, val := range entries {
		switch {
		case strings.HasPrefix(key, stringPrefix):
			name := key[len(stringPrefix):]
			candidatesByName[name] = append(candidatesByName[name], candidate{
				kind: typeString,
				v:    &value{kind: typeString, str: string(val)},
			})
		case strings.HasPrefix(key, hashPrefix):
			name := key[len(hashPrefix):]
			fields := make(map[string]string)
			if err := json.Unmarshal(val, &fields); err != nil {
				skipped = append(skipped, key)
				continue
			}
			candidatesByName[name] = append(candidatesByName[name], candidate{
				kind: typeHash,
				v:    &value{kind: typeHash, hash: fields},
			})
		case strings.HasPrefix(key, setPrefix):
			name := key[len(setPrefix):]
			var names []string
			if err := json.Unmarshal(val, &names); err != nil {
				skipped = append(skipped, key)
				continue
			}
			members := make(map[string]struct{}, len(names))
			for _, member := range names {
				members[member] = struct{}{}
			}
			candidatesByName[name] = append(candidatesByName[name], candidate{
				kind: typeSet,
				v:    &value{kind: typeSet, set: members},
			})
		case strings.HasPrefix(key, listPrefix):
			name := key[len(listPrefix):]
			var items []string
			if err := json.Unmarshal(val, &items); err != nil {
				skipped = append(skipped, key)
				continue
			}
			candidatesByName[name] = append(candidatesByName[name], candidate{
				kind: typeList,
				v:    &value{kind: typeList, list: items},
			})
		case strings.HasPrefix(key, zsetPrefix):
			name := key[len(zsetPrefix):]
			members := make(map[string]float64)
			if err := json.Unmarshal(val, &members); err != nil {
				skipped = append(skipped, key)
				continue
			}
			candidatesByName[name] = append(candidatesByName[name], candidate{
				kind: typeZSet,
				v:    &value{kind: typeZSet, zset: members},
			})
		case strings.HasPrefix(key, expirePrefix):
			name := key[len(expirePrefix):]
			ms, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				skipped = append(skipped, key)
				continue
			}
			expireByName[name] = ms
		default:
			skipped = append(skipped, key)
		}
	}

	for name, candidates := range candidatesByName {
		best := candidates[0]
		for _, c := range candidates[1:] {
			// Lower valueType value is higher precedence: string(0) > hash(1) > set(2).
			if c.kind < best.kind {
				best = c
			}
		}
		store.keys[name] = best.v
		for _, c := range candidates {
			if c.kind != best.kind {
				losers = append(losers, c.kind.prefix()+name)
			}
		}
	}

	for name, ms := range expireByName {
		if v, ok := store.keys[name]; ok {
			v.expireAt = ms
		} else {
			orphanedExpiry = append(orphanedExpiry, expirePrefix+name)
		}
	}

	sort.Strings(losers)
	sort.Strings(skipped)
	sort.Strings(orphanedExpiry)
	return store, losers, skipped, orphanedExpiry
}
