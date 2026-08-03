package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// seed creates a key of the given kind via the real command handlers, so the
// WRONGTYPE matrix below exercises the same path a client would.
func seed(t *testing.T, store *Datastore, key string, kind valueType) {
	t.Helper()
	switch kind {
	case typeString:
		assert.Equal(t, okReply, setCommand(store, []string{key, "v"}))
	case typeHash:
		assert.Equal(t, ":1\r\n", hsetCommand(store, []string{key, "f", "v"}))
	case typeSet:
		assert.Equal(t, ":1\r\n", saddCommand(store, []string{key, "m"}))
	}
}

func TestWrongType(t *testing.T) {
	// nativeKind is the kind each command operates on; against a key holding
	// any other kind it must reply WRONGTYPE instead of touching the value.
	commands := []struct {
		name       string
		nativeKind valueType
		call       func(store *Datastore, key string) string
	}{
		{"GET", typeString, func(store *Datastore, key string) string {
			return getCommand(store, []string{key})
		}},
		{"HSET", typeHash, func(store *Datastore, key string) string {
			return hsetCommand(store, []string{key, "f", "v"})
		}},
		{"HGET", typeHash, func(store *Datastore, key string) string {
			return hgetCommand(store, []string{key, "f"})
		}},
		{"SADD", typeSet, func(store *Datastore, key string) string {
			return saddCommand(store, []string{key, "m"})
		}},
		{"HDEL", typeHash, func(store *Datastore, key string) string {
			return hdelCommand(store, []string{key, "f"})
		}},
		{"SREM", typeSet, func(store *Datastore, key string) string {
			return sremCommand(store, []string{key, "m"})
		}},
	}
	kinds := []valueType{typeString, typeHash, typeSet}

	for _, seedKind := range kinds {
		for _, cmd := range commands {
			name := cmd.name + " against " + seedKind.String()
			t.Run(name, func(t *testing.T) {
				store := NewDatastore()
				seed(t, store, "k", seedKind)

				got := cmd.call(store, "k")
				if cmd.nativeKind == seedKind {
					assert.NotEqual(t, errWrongType, got)
				} else {
					assert.Equal(t, errWrongType, got)
				}
			})
		}
	}

	t.Run("absent key never WRONGTYPEs", func(t *testing.T) {
		for _, cmd := range commands {
			store := NewDatastore()
			assert.NotEqual(t, errWrongType, cmd.call(store, "missing"))
		}
	})
}

func TestSetOverwritesHashRecordsStale(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, ":1\r\n", hsetCommand(store, []string{"k", "f", "v"}))

	assert.Equal(t, okReply, setCommand(store, []string{"k", "s"}))

	assert.Contains(t, store.stale, hashPrefix+"k")
	v, found, errReply := store.lookup("k", typeString)
	assert.Empty(t, errReply)
	assert.True(t, found)
	assert.Equal(t, "s", v.str)
}

func TestSetOverwritesSetRecordsStale(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, ":1\r\n", saddCommand(store, []string{"k", "m"}))

	assert.Equal(t, okReply, setCommand(store, []string{"k", "s"}))

	assert.Contains(t, store.stale, setPrefix+"k")
	v, found, errReply := store.lookup("k", typeString)
	assert.Empty(t, errReply)
	assert.True(t, found)
	assert.Equal(t, "s", v.str)
}

func TestReplaceClearsMatchingStaleEntry(t *testing.T) {
	store := NewDatastore()
	// Simulate a leftover stale marker for the same prefixed name replace is
	// about to (re)write, e.g. left behind by a hypothetical prior DEL.
	store.stale[stringPrefix+"k"] = struct{}{}

	store.replace("k", &value{kind: typeString, str: "v"})

	assert.NotContains(t, store.stale, stringPrefix+"k")
}

func TestMutableClearsMatchingStaleEntry(t *testing.T) {
	store := NewDatastore()
	store.stale[hashPrefix+"k"] = struct{}{}

	v, errReply := store.mutable("k", typeHash)

	assert.Empty(t, errReply)
	assert.NotNil(t, v)
	assert.NotContains(t, store.stale, hashPrefix+"k")
}

func TestTypeCommand(t *testing.T) {
	tests := []struct {
		name string
		seed valueType
		want string
	}{
		{"string", typeString, "+string\r\n"},
		{"hash", typeHash, "+hash\r\n"},
		{"set", typeSet, "+set\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewDatastore()
			seed(t, store, "k", tt.seed)
			assert.Equal(t, tt.want, typeCommand(store, []string{"k"}))
		})
	}

	t.Run("absent key", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, noneReply, typeCommand(store, []string{"missing"}))
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, wrongArgs("type"), typeCommand(store, nil))
	})
}

func TestReplaceRecordsPreviousKindAsStaleOnlyOnKindChange(t *testing.T) {
	store := NewDatastore()
	store.replace("k", &value{kind: typeString, str: "a"})

	// Same kind: overwriting a string with another string must not stale it.
	store.replace("k", &value{kind: typeString, str: "b"})
	assert.NotContains(t, store.stale, stringPrefix+"k")

	// Different kind: the previous (string) prefixed key becomes stale.
	store.replace("k", &value{kind: typeHash, hash: map[string]string{"f": "v"}})
	assert.Contains(t, store.stale, stringPrefix+"k")
	assert.NotContains(t, store.stale, hashPrefix+"k")
}

func TestDelCommand(t *testing.T) {
	// DEL is type-agnostic: whatever the key holds, it goes, and the prefixed
	// name it occupied on disk is staled so the next save drops it too.
	for _, kind := range []valueType{typeString, typeHash, typeSet} {
		t.Run("removes a "+kind.String(), func(t *testing.T) {
			store := NewDatastore()
			seed(t, store, "k", kind)

			assert.Equal(t, integer(1), delCommand(store, []string{"k"}))

			assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
			assert.Contains(t, store.stale, kind.prefix()+"k")
		})
	}

	t.Run("counts only the keys that existed", func(t *testing.T) {
		store := NewDatastore()
		seed(t, store, "a", typeString)
		seed(t, store, "b", typeSet)

		assert.Equal(t, integer(2), delCommand(store, []string{"a", "missing", "b"}))
		assert.Empty(t, store.keys)
	})

	t.Run("absent key is a no-op, not a stale entry", func(t *testing.T) {
		store := NewDatastore()

		assert.Equal(t, integer(0), delCommand(store, []string{"missing"}))
		assert.Empty(t, store.stale)
	})

	t.Run("repeated key in one call counts once", func(t *testing.T) {
		store := NewDatastore()
		seed(t, store, "k", typeString)

		assert.Equal(t, integer(1), delCommand(store, []string{"k", "k"}))
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, wrongArgs("del"), delCommand(store, nil))
	})
}

// TestDelThenRecreateClearsStale covers the ordering hazard the stale
// mechanism exists for: a prefixed name must not be both stale and live, or
// saveToDisk's delete pass would race the insert that re-creates it.
func TestDelThenRecreateClearsStale(t *testing.T) {
	t.Run("recreated by SET", func(t *testing.T) {
		store := NewDatastore()
		seed(t, store, "k", typeString)
		delCommand(store, []string{"k"})
		assert.Contains(t, store.stale, stringPrefix+"k")

		assert.Equal(t, okReply, setCommand(store, []string{"k", "again"}))
		assert.NotContains(t, store.stale, stringPrefix+"k")
	})

	t.Run("recreated by HSET", func(t *testing.T) {
		store := NewDatastore()
		seed(t, store, "k", typeHash)
		delCommand(store, []string{"k"})
		assert.Contains(t, store.stale, hashPrefix+"k")

		assert.Equal(t, ":1\r\n", hsetCommand(store, []string{"k", "f", "v"}))
		assert.NotContains(t, store.stale, hashPrefix+"k")
	})
}

func TestHdelCommand(t *testing.T) {
	t.Run("removes fields and counts only those present", func(t *testing.T) {
		store := NewDatastore()
		hsetCommand(store, []string{"k", "f1", "v1"})
		hsetCommand(store, []string{"k", "f2", "v2"})

		assert.Equal(t, integer(1), hdelCommand(store, []string{"k", "f1", "missing"}))

		assert.Equal(t, nilBulk, hgetCommand(store, []string{"k", "f1"}))
		assert.Equal(t, bulkString("v2"), hgetCommand(store, []string{"k", "f2"}))
	})

	t.Run("surviving fields keep the key alive and unstaled", func(t *testing.T) {
		store := NewDatastore()
		hsetCommand(store, []string{"k", "f1", "v1"})
		hsetCommand(store, []string{"k", "f2", "v2"})

		hdelCommand(store, []string{"k", "f1"})

		assert.Equal(t, "+hash\r\n", typeCommand(store, []string{"k"}))
		assert.Empty(t, store.stale)
	})

	t.Run("emptying the hash removes the key", func(t *testing.T) {
		store := NewDatastore()
		hsetCommand(store, []string{"k", "f", "v"})

		assert.Equal(t, integer(1), hdelCommand(store, []string{"k", "f"}))

		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
		assert.Contains(t, store.stale, hashPrefix+"k")
	})

	t.Run("absent key replies zero without staling", func(t *testing.T) {
		store := NewDatastore()

		assert.Equal(t, integer(0), hdelCommand(store, []string{"missing", "f"}))
		assert.Empty(t, store.stale)
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, wrongArgs("hdel"), hdelCommand(store, []string{"k"}))
	})
}

func TestSremCommand(t *testing.T) {
	t.Run("removes members and counts only those present", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"k", "m1", "m2"})

		assert.Equal(t, integer(1), sremCommand(store, []string{"k", "m1", "missing"}))

		v, found, errReply := store.lookup("k", typeSet)
		assert.Empty(t, errReply)
		assert.True(t, found)
		assert.Equal(t, map[string]struct{}{"m2": {}}, v.set)
	})

	t.Run("surviving members keep the key alive and unstaled", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"k", "m1", "m2"})

		sremCommand(store, []string{"k", "m1"})

		assert.Equal(t, "+set\r\n", typeCommand(store, []string{"k"}))
		assert.Empty(t, store.stale)
	})

	t.Run("emptying the set removes the key", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"k", "m"})

		assert.Equal(t, integer(1), sremCommand(store, []string{"k", "m"}))

		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
		assert.Contains(t, store.stale, setPrefix+"k")
	})

	t.Run("absent key replies zero without staling", func(t *testing.T) {
		store := NewDatastore()

		assert.Equal(t, integer(0), sremCommand(store, []string{"missing", "m"}))
		assert.Empty(t, store.stale)
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, wrongArgs("srem"), sremCommand(store, []string{"k"}))
	})
}

// TestDelIsTypeAgnostic pins the one deliberate exception to the WRONGTYPE
// matrix above: DEL takes any key, so it must never reply WRONGTYPE.
func TestDelIsTypeAgnostic(t *testing.T) {
	for _, kind := range []valueType{typeString, typeHash, typeSet} {
		store := NewDatastore()
		seed(t, store, "k", kind)
		assert.Equal(t, integer(1), delCommand(store, []string{"k"}))
	}
}

// TestDeletedKeyLeavesSnapshot is the disk-facing half of the contract: a
// deleted key must be gone from what snapshot writes *and* present in stale so
// saveToDisk removes the name BatchInsert can no longer overwrite.
func TestDeletedKeyLeavesSnapshot(t *testing.T) {
	store := NewDatastore()
	seed(t, store, "gone", typeHash)
	seed(t, store, "kept", typeString)

	delCommand(store, []string{"gone"})

	entries := snapshot(store)
	assert.NotContains(t, entries, hashPrefix+"gone")
	assert.Contains(t, entries, stringPrefix+"kept")
	assert.Equal(t, map[string]struct{}{hashPrefix + "gone": {}}, store.stale)
}
