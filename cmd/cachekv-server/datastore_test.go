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
