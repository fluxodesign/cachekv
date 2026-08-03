package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/fluxodesign/cachekv/cachekv"
)

const (
	stringPrefix = "s:"
	hashPrefix   = "h:"
	setPrefix    = "z:"

	// Reserved for future types, so nobody reaches for z: when sorted sets
	// arrive: l: lists, o: sorted sets (ordered). z: stays on sets despite
	// the naming clash — see plan-unify-datastore-keyspace.md.
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
	default:
		return typeString
	}
}

// snapshot flattens store's unified keyspace into a single key->value set
// suitable for cachekv.BatchInsert, prefixing each key by its logical type so
// they can share one namespace without colliding.
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

	store, losers, skipped := reconcile(all)
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
func reconcile(entries map[string][]byte) (store *Datastore, losers []string, skipped []string) {
	store = NewDatastore()

	type candidate struct {
		kind valueType
		v    *value
	}
	candidatesByName := make(map[string][]candidate)

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

	sort.Strings(losers)
	sort.Strings(skipped)
	return store, losers, skipped
}
