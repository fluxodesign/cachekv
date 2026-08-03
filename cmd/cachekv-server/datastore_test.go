package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

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
	case typeList:
		assert.Equal(t, integer(1), rpushCommand(store, []string{key, "v"}))
	case typeZSet:
		assert.Equal(t, integer(1), zaddCommand(store, []string{key, "1", "m"}))
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
		{"LRANGE", typeList, func(store *Datastore, key string) string {
			return lrangeCommand(store, []string{key, "0", "-1"})
		}},
		{"LPUSH", typeList, func(store *Datastore, key string) string {
			return lpushCommand(store, []string{key, "v"})
		}},
		{"ZSCORE", typeZSet, func(store *Datastore, key string) string {
			return zscoreCommand(store, []string{key, "m"})
		}},
		{"ZADD", typeZSet, func(store *Datastore, key string) string {
			return zaddCommand(store, []string{key, "1", "m"})
		}},
	}
	kinds := []valueType{typeString, typeHash, typeSet, typeList, typeZSet}

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

func TestSelectCommand(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, okReply, selectCommand(store, []string{"0"}))
	assert.Equal(t, "-ERR DB index is out of range\r\n", selectCommand(store, []string{"1"}))
	assert.Equal(t, wrongArgs("select"), selectCommand(store, nil))
}

func TestConfigGet(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, arrayReply(bulkString("maxmemory"), bulkString("0")),
		configCommand(store, []string{"GET", "maxmemory"}))

	// A glob pattern matches every known key that starts with "a", sorted.
	assert.Equal(t, arrayReply(
		bulkString("appendonly"), bulkString("no"),
	), configCommand(store, []string{"GET", "a*"}))

	assert.Equal(t, emptyArray, configCommand(store, []string{"GET", "nosuchkey"}))
}

func TestConfigSet(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, okReply, configCommand(store, []string{"SET", "maxmemory", "100"}))
	assert.Equal(t, arrayReply(bulkString("maxmemory"), bulkString("100")),
		configCommand(store, []string{"GET", "maxmemory"}))

	assert.Equal(t, "-ERR Unknown CONFIG parameter 'nosuch'\r\n",
		configCommand(store, []string{"SET", "nosuch", "x"}))
}

func TestConfigUnknownSubcommand(t *testing.T) {
	store := NewDatastore()
	assert.Contains(t, configCommand(store, []string{"NOPE"}), "-ERR")
	assert.Equal(t, wrongArgs("config"), configCommand(store, nil))
}

func TestCommandCount(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer(len(allCommandNames())), commandCommand(store, []string{"COUNT"}))
}

func TestCommandDocsIsEmpty(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, emptyArray, commandCommand(store, []string{"DOCS"}))
	assert.Equal(t, emptyArray, commandCommand(store, []string{"DOCS", "GET"}))
}

func TestCommandListContainsKnownCommands(t *testing.T) {
	store := NewDatastore()
	reply := commandCommand(store, nil)

	assert.True(t, strings.HasPrefix(reply, fmt.Sprintf("*%d\r\n", len(allCommandNames()))))
	assert.Contains(t, reply, "$3\r\nGET\r\n")
	assert.Contains(t, reply, "$4\r\nAUTH\r\n")
}

func TestCommandUnknownSubcommand(t *testing.T) {
	store := NewDatastore()
	assert.Contains(t, commandCommand(store, []string{"NOPE"}), "-ERR")
}

// clockAt builds a store whose clock is pinned to t, so expiry tests control
// time directly instead of sleeping.
func clockAt(t time.Time) *Datastore {
	store := NewDatastore()
	store.now = func() time.Time { return t }
	return store
}

func TestTTLPTTL(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	assert.Equal(t, integer(-2), ttlCommand(store, []string{"missing"}))
	assert.Equal(t, integer(-2), pttlCommand(store, []string{"missing"}))

	setCommand(store, []string{"nottl", "v"})
	assert.Equal(t, integer(-1), ttlCommand(store, []string{"nottl"}))
	assert.Equal(t, integer(-1), pttlCommand(store, []string{"nottl"}))

	setCommand(store, []string{"k", "v", "EX", "10"})
	assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))
	assert.Equal(t, integer(10000), pttlCommand(store, []string{"k"}))

	assert.Equal(t, wrongArgs("ttl"), ttlCommand(store, nil))
	assert.Equal(t, wrongArgs("pttl"), pttlCommand(store, nil))
}

func TestExpireFamilySetsAndDeletesPastTargets(t *testing.T) {
	// Truncated to a whole second: EXPIREAT's target is seconds-resolution,
	// so a sub-second now would make the resulting TTL depend on now's
	// fractional part (and occasionally round down a second), flaking on
	// wall-clock timing rather than testing EXPIREAT itself.
	now := time.Now().Truncate(time.Second)
	store := clockAt(now)

	setCommand(store, []string{"k", "v"})
	assert.Equal(t, integer(1), expireCommand(store, []string{"k", "10"}))
	assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))

	assert.Equal(t, integer(1), pexpireCommand(store, []string{"k", "5000"}))
	assert.Equal(t, integer(5), ttlCommand(store, []string{"k"}))

	assert.Equal(t, integer(1), expireatCommand(store, []string{"k", strconv.FormatInt(now.Add(20*time.Second).Unix(), 10)}))
	assert.Equal(t, integer(20), ttlCommand(store, []string{"k"}))

	// A target already due deletes the key and still reports 1, matching
	// Redis: EXPIRE key 0 (or any past time) is equivalent to DEL.
	assert.Equal(t, integer(1), expireCommand(store, []string{"k", "0"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	assert.Contains(t, store.stale, expirePrefix+"k")

	assert.Equal(t, integer(0), expireCommand(store, []string{"missing", "10"}))
	assert.Equal(t, wrongArgs("expire"), expireCommand(store, []string{"k"}))
	assert.Equal(t, errNotInteger, expireCommand(store, []string{"k", "notanumber"}))
}

func TestExpireConditions(t *testing.T) {
	now := time.Now()

	t.Run("NX only applies with no existing TTL", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "10"})
		assert.Equal(t, integer(0), expireCommand(store, []string{"k", "20", "NX"}))
		assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))

		setCommand(store, []string{"j", "v"})
		assert.Equal(t, integer(1), expireCommand(store, []string{"j", "20", "NX"}))
	})

	t.Run("XX only applies with an existing TTL", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v"})
		assert.Equal(t, integer(0), expireCommand(store, []string{"k", "20", "XX"}))

		setCommand(store, []string{"k", "v", "EX", "10"})
		assert.Equal(t, integer(1), expireCommand(store, []string{"k", "20", "XX"}))
	})

	t.Run("GT only applies when new TTL is greater", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "10"})
		assert.Equal(t, integer(0), expireCommand(store, []string{"k", "5", "GT"}))
		assert.Equal(t, integer(1), expireCommand(store, []string{"k", "20", "GT"}))

		// No existing TTL is "infinite" for GT: nothing is ever greater.
		setCommand(store, []string{"j", "v"})
		assert.Equal(t, integer(0), expireCommand(store, []string{"j", "1000", "GT"}))
	})

	t.Run("LT only applies when new TTL is smaller", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "10"})
		assert.Equal(t, integer(0), expireCommand(store, []string{"k", "20", "LT"}))
		assert.Equal(t, integer(1), expireCommand(store, []string{"k", "5", "LT"}))

		// No existing TTL is "infinite" for LT: anything finite is smaller.
		setCommand(store, []string{"j", "v"})
		assert.Equal(t, integer(1), expireCommand(store, []string{"j", "1000", "LT"}))
	})

	t.Run("unsupported option", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v"})
		assert.Contains(t, expireCommand(store, []string{"k", "10", "BOGUS"}), "-ERR")
	})
}

func TestPersistCommand(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	setCommand(store, []string{"k", "v", "EX", "10"})
	assert.Equal(t, integer(1), persistCommand(store, []string{"k"}))
	assert.Equal(t, integer(-1), ttlCommand(store, []string{"k"}))
	assert.Contains(t, store.stale, expirePrefix+"k")

	assert.Equal(t, integer(0), persistCommand(store, []string{"k"}))
	assert.Equal(t, integer(0), persistCommand(store, []string{"missing"}))
	assert.Equal(t, wrongArgs("persist"), persistCommand(store, nil))
}

func TestExpiretimeCommand(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	assert.Equal(t, integer(-2), expiretimeCommand(store, []string{"missing"}))

	setCommand(store, []string{"nottl", "v"})
	assert.Equal(t, integer(-1), expiretimeCommand(store, []string{"nottl"}))

	setCommand(store, []string{"k", "v", "EX", "10"})
	assert.Equal(t, integer(int(now.Add(10*time.Second).Unix())), expiretimeCommand(store, []string{"k"}))
}

func TestSetexPsetex(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	assert.Contains(t, setexCommand(store, []string{"k", "0", "v"}), "invalid expire time")
	assert.Contains(t, setexCommand(store, []string{"k", "-1", "v"}), "invalid expire time")

	assert.Equal(t, okReply, setexCommand(store, []string{"k", "10", "v"}))
	assert.Equal(t, bulkString("v"), getCommand(store, []string{"k"}))
	assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))

	assert.Equal(t, okReply, psetexCommand(store, []string{"k", "5000", "v2"}))
	assert.Equal(t, integer(5), ttlCommand(store, []string{"k"}))

	assert.Equal(t, wrongArgs("setex"), setexCommand(store, []string{"k", "10"}))
}

func TestGetexNoOptionsLeavesTTLUntouched(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	setCommand(store, []string{"k", "v", "EX", "10"})
	assert.Equal(t, bulkString("v"), getexCommand(store, []string{"k"}))
	assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))

	assert.Equal(t, nilBulk, getexCommand(store, []string{"missing"}))
}

func TestGetexMutatesTTL(t *testing.T) {
	now := time.Now()
	store := clockAt(now)
	setCommand(store, []string{"k", "v"})

	assert.Equal(t, bulkString("v"), getexCommand(store, []string{"k", "EX", "10"}))
	assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))

	assert.Equal(t, bulkString("v"), getexCommand(store, []string{"k", "PERSIST"}))
	assert.Equal(t, integer(-1), ttlCommand(store, []string{"k"}))

	// A past target deletes the key but still returns the value read.
	assert.Equal(t, bulkString("v"), getexCommand(store, []string{"k", "EXAT", "1"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
}

func TestSetOptions(t *testing.T) {
	now := time.Now()

	t.Run("plain 2-arg call is unchanged: no TTL", func(t *testing.T) {
		store := clockAt(now)
		assert.Equal(t, okReply, setCommand(store, []string{"k", "v"}))
		assert.Equal(t, integer(-1), ttlCommand(store, []string{"k"}))
	})

	t.Run("NX fails when key exists", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v"})
		assert.Equal(t, nilBulk, setCommand(store, []string{"k", "v2", "NX"}))
		assert.Equal(t, bulkString("v"), getCommand(store, []string{"k"}))
	})

	t.Run("XX fails when key absent", func(t *testing.T) {
		store := clockAt(now)
		assert.Equal(t, nilBulk, setCommand(store, []string{"k", "v", "XX"}))
	})

	t.Run("EX sets a TTL", func(t *testing.T) {
		store := clockAt(now)
		assert.Equal(t, okReply, setCommand(store, []string{"k", "v", "EX", "10"}))
		assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))
	})

	t.Run("plain SET clears a prior TTL", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "10"})
		setCommand(store, []string{"k", "v2"})
		assert.Equal(t, integer(-1), ttlCommand(store, []string{"k"}))
		assert.Contains(t, store.stale, expirePrefix+"k")
	})

	t.Run("KEEPTTL preserves a prior TTL", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "10"})
		setCommand(store, []string{"k", "v2", "KEEPTTL"})
		assert.Equal(t, integer(10), ttlCommand(store, []string{"k"}))
		assert.Equal(t, bulkString("v2"), getCommand(store, []string{"k"}))
	})

	t.Run("a past EXAT target deletes immediately", func(t *testing.T) {
		store := clockAt(now)
		setCommand(store, []string{"k", "old"})
		assert.Equal(t, okReply, setCommand(store, []string{"k", "v", "EXAT", "1"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	})

	t.Run("conflicting and unknown options are syntax errors", func(t *testing.T) {
		store := clockAt(now)
		assert.Equal(t, errSyntax, setCommand(store, []string{"k", "v", "NX", "XX"}))
		assert.Equal(t, errSyntax, setCommand(store, []string{"k", "v", "EX", "10", "KEEPTTL"}))
		assert.Equal(t, errSyntax, setCommand(store, []string{"k", "v", "EX", "10", "PX", "10"}))
		assert.Equal(t, errSyntax, setCommand(store, []string{"k", "v", "BOGUS"}))
	})
}

func TestLazyExpiryIsInvisibleWithoutAnySweep(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	setCommand(store, []string{"k", "v", "PX", "1"})
	hsetCommand(store, []string{"h", "f", "v"})
	pexpireCommand(store, []string{"h", "1"})

	store.now = func() time.Time { return now.Add(5 * time.Millisecond) }

	assert.Equal(t, nilBulk, getCommand(store, []string{"k"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	assert.Equal(t, nilBulk, hgetCommand(store, []string{"h", "f"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"h"}))
}

func TestExpireCycleRemovesDueKeysAndIncrementsDirtyOnlyWhenSomethingRemoved(t *testing.T) {
	now := time.Now()
	store := clockAt(now)

	setCommand(store, []string{"due", "v", "PX", "1"})
	setCommand(store, []string{"notdue", "v", "EX", "100"})

	store.now = func() time.Time { return now.Add(5 * time.Millisecond) }

	cmdChan := make(chan CkvCommand)
	go stateProcessor(cmdChan, store, "unused-db", 1<<30) // saveMinChanges effectively unreachable
	defer close(cmdChan)

	cmdChan <- CkvCommand{Op: "EXPIRECYCLE"}

	// stateProcessor handles commands strictly in receive order on this one
	// channel, so waiting for a following command's reply proves EXPIRECYCLE
	// has already finished mutating store.
	resp := make(chan string, 1)
	cmdChan <- CkvCommand{Op: "PING", Resp: resp}
	<-resp

	assert.Equal(t, noneReply, typeCommand(store, []string{"due"}))
	assert.Equal(t, "+string\r\n", typeCommand(store, []string{"notdue"}))
}

func TestLpushRpushOrdering(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, integer(3), lpushCommand(store, []string{"k", "a", "b", "c"}))
	assert.Equal(t, arrayReply(bulkString("c"), bulkString("b"), bulkString("a")),
		lrangeCommand(store, []string{"k", "0", "-1"}))

	store2 := NewDatastore()
	assert.Equal(t, integer(3), rpushCommand(store2, []string{"k", "a", "b", "c"}))
	assert.Equal(t, arrayReply(bulkString("a"), bulkString("b"), bulkString("c")),
		lrangeCommand(store2, []string{"k", "0", "-1"}))
}

func TestLpopRpop(t *testing.T) {
	t.Run("single pop without count", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "c"})

		assert.Equal(t, bulkString("a"), lpopCommand(store, []string{"k"}))
		assert.Equal(t, bulkString("c"), rpopCommand(store, []string{"k"}))
		assert.Equal(t, nilBulk, lpopCommand(store, []string{"missing"}))
	})

	t.Run("pop with count returns an array, tail-first for RPOP", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "c", "d"})

		assert.Equal(t, arrayReply(bulkString("a"), bulkString("b")), lpopCommand(store, []string{"k", "2"}))

		store2 := NewDatastore()
		rpushCommand(store2, []string{"k", "a", "b", "c", "d"})
		assert.Equal(t, arrayReply(bulkString("d"), bulkString("c")), rpopCommand(store2, []string{"k", "2"}))
	})

	t.Run("count on an absent key is nilArray, not emptyArray", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, nilArray, lpopCommand(store, []string{"missing", "2"}))
	})

	t.Run("count larger than the list is clamped", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b"})
		assert.Equal(t, arrayReply(bulkString("a"), bulkString("b")), lpopCommand(store, []string{"k", "10"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	})

	t.Run("negative count is rejected", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a"})
		assert.Equal(t, errMustBePositive, lpopCommand(store, []string{"k", "-1"}))
	})

	t.Run("popping to empty removes the key", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a"})
		lpopCommand(store, []string{"k"})
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	})
}

func TestLrangeLtrimNormalizeRange(t *testing.T) {
	store := NewDatastore()
	rpushCommand(store, []string{"k", "a", "b", "c", "d", "e"})

	assert.Equal(t, arrayReply(bulkString("c"), bulkString("d"), bulkString("e")),
		lrangeCommand(store, []string{"k", "-3", "-1"}))
	assert.Equal(t, emptyArray, lrangeCommand(store, []string{"k", "10", "20"}))
	assert.Equal(t, emptyArray, lrangeCommand(store, []string{"missing", "0", "-1"}))

	assert.Equal(t, okReply, ltrimCommand(store, []string{"k", "1", "3"}))
	assert.Equal(t, arrayReply(bulkString("b"), bulkString("c"), bulkString("d")),
		lrangeCommand(store, []string{"k", "0", "-1"}))

	assert.Equal(t, okReply, ltrimCommand(store, []string{"missing", "0", "-1"}))

	assert.Equal(t, okReply, ltrimCommand(store, []string{"k", "5", "10"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
}

func TestLlen(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer(0), llenCommand(store, []string{"missing"}))
	rpushCommand(store, []string{"k", "a", "b"})
	assert.Equal(t, integer(2), llenCommand(store, []string{"k"}))
}

func TestLrem(t *testing.T) {
	t.Run("positive count removes head-to-tail", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "a", "c", "a"})
		assert.Equal(t, integer(2), lremCommand(store, []string{"k", "2", "a"}))
		assert.Equal(t, arrayReply(bulkString("b"), bulkString("c"), bulkString("a")),
			lrangeCommand(store, []string{"k", "0", "-1"}))
	})

	t.Run("negative count removes tail-to-head", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "a", "c", "a"})
		assert.Equal(t, integer(2), lremCommand(store, []string{"k", "-2", "a"}))
		assert.Equal(t, arrayReply(bulkString("a"), bulkString("b"), bulkString("c")),
			lrangeCommand(store, []string{"k", "0", "-1"}))
	})

	t.Run("zero count removes all occurrences", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "a", "c", "a"})
		assert.Equal(t, integer(3), lremCommand(store, []string{"k", "0", "a"}))
		assert.Equal(t, arrayReply(bulkString("b"), bulkString("c")),
			lrangeCommand(store, []string{"k", "0", "-1"}))
	})

	t.Run("absent key", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(0), lremCommand(store, []string{"missing", "0", "a"}))
	})
}

func TestLset(t *testing.T) {
	store := NewDatastore()
	rpushCommand(store, []string{"k", "a", "b", "c"})

	assert.Equal(t, okReply, lsetCommand(store, []string{"k", "1", "B"}))
	assert.Equal(t, arrayReply(bulkString("a"), bulkString("B"), bulkString("c")),
		lrangeCommand(store, []string{"k", "0", "-1"}))

	assert.Equal(t, okReply, lsetCommand(store, []string{"k", "-1", "C"}))
	assert.Equal(t, arrayReply(bulkString("C")), lrangeCommand(store, []string{"k", "-1", "-1"}))

	assert.Equal(t, "-ERR index out of range\r\n", lsetCommand(store, []string{"k", "10", "x"}))
	assert.Equal(t, "-ERR no such key\r\n", lsetCommand(store, []string{"missing", "0", "x"}))
}

func TestLinsert(t *testing.T) {
	store := NewDatastore()
	rpushCommand(store, []string{"k", "a", "c"})

	assert.Equal(t, integer(3), linsertCommand(store, []string{"k", "BEFORE", "c", "b"}))
	assert.Equal(t, arrayReply(bulkString("a"), bulkString("b"), bulkString("c")),
		lrangeCommand(store, []string{"k", "0", "-1"}))

	assert.Equal(t, integer(4), linsertCommand(store, []string{"k", "AFTER", "c", "d"}))
	assert.Equal(t, arrayReply(bulkString("a"), bulkString("b"), bulkString("c"), bulkString("d")),
		lrangeCommand(store, []string{"k", "0", "-1"}))

	assert.Equal(t, integer(-1), linsertCommand(store, []string{"k", "BEFORE", "nosuch", "x"}))
	assert.Equal(t, integer(0), linsertCommand(store, []string{"missing", "BEFORE", "a", "x"}))
	assert.Equal(t, errSyntax, linsertCommand(store, []string{"k", "SOMEWHERE", "a", "x"}))
}

func TestLmove(t *testing.T) {
	t.Run("between two different keys", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"src", "a", "b", "c"})

		assert.Equal(t, bulkString("a"), lmoveCommand(store, []string{"src", "dst", "LEFT", "RIGHT"}))
		assert.Equal(t, arrayReply(bulkString("b"), bulkString("c")), lrangeCommand(store, []string{"src", "0", "-1"}))
		assert.Equal(t, arrayReply(bulkString("a")), lrangeCommand(store, []string{"dst", "0", "-1"}))
	})

	t.Run("rotate in place when source equals destination", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"k", "a", "b", "c"})

		assert.Equal(t, bulkString("a"), lmoveCommand(store, []string{"k", "k", "LEFT", "RIGHT"}))
		assert.Equal(t, arrayReply(bulkString("b"), bulkString("c"), bulkString("a")),
			lrangeCommand(store, []string{"k", "0", "-1"}))
	})

	t.Run("WRONGTYPE on destination leaves source untouched", func(t *testing.T) {
		store := NewDatastore()
		rpushCommand(store, []string{"src", "a", "b"})
		setCommand(store, []string{"dst", "not-a-list"})

		assert.Equal(t, errWrongType, lmoveCommand(store, []string{"src", "dst", "LEFT", "RIGHT"}))
		assert.Equal(t, arrayReply(bulkString("a"), bulkString("b")), lrangeCommand(store, []string{"src", "0", "-1"}))
	})

	t.Run("absent source", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, nilBulk, lmoveCommand(store, []string{"missing", "dst", "LEFT", "RIGHT"}))
	})
}

func TestZadd(t *testing.T) {
	t.Run("plain add counts new members", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(2), zaddCommand(store, []string{"k", "1", "a", "2", "b"}))
		assert.Equal(t, bulkString("1"), zscoreCommand(store, []string{"k", "a"}))
	})

	t.Run("NX skips existing members", func(t *testing.T) {
		store := NewDatastore()
		zaddCommand(store, []string{"k", "1", "a"})
		assert.Equal(t, integer(0), zaddCommand(store, []string{"k", "NX", "99", "a"}))
		assert.Equal(t, bulkString("1"), zscoreCommand(store, []string{"k", "a"}))
	})

	t.Run("XX on a wholly new key does not create it", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(0), zaddCommand(store, []string{"k", "XX", "1", "a"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	})

	t.Run("XX only updates existing members", func(t *testing.T) {
		store := NewDatastore()
		zaddCommand(store, []string{"k", "1", "a"})
		assert.Equal(t, integer(0), zaddCommand(store, []string{"k", "XX", "5", "b"}))
		assert.Equal(t, nilBulk, zscoreCommand(store, []string{"k", "b"}))
		assert.Equal(t, integer(0), zaddCommand(store, []string{"k", "XX", "5", "a"}))
		assert.Equal(t, bulkString("5"), zscoreCommand(store, []string{"k", "a"}))
	})

	t.Run("CH counts updates too, not just additions", func(t *testing.T) {
		store := NewDatastore()
		zaddCommand(store, []string{"k", "1", "a"})
		// a's score changes (1->2) and b is newly added: both count under CH.
		assert.Equal(t, integer(2), zaddCommand(store, []string{"k", "CH", "2", "a", "1", "b"}))
	})

	t.Run("NX and XX together is a syntax error", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, errSyntax, zaddCommand(store, []string{"k", "NX", "XX", "1", "a"}))
	})

	t.Run("odd number of score/member args", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, wrongArgs("zadd"), zaddCommand(store, []string{"k", "1"}))
	})

	t.Run("non-float score", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, errNotFloat, zaddCommand(store, []string{"k", "notanumber", "a"}))
	})
}

func TestZscoreZcardZrank(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, nilBulk, zscoreCommand(store, []string{"missing", "a"}))
	assert.Equal(t, integer(0), zcardCommand(store, []string{"missing"}))
	assert.Equal(t, nilBulk, zrankCommand(store, []string{"missing", "a"}))

	zaddCommand(store, []string{"k", "3", "a", "1", "b", "2", "c"})
	assert.Equal(t, integer(3), zcardCommand(store, []string{"k"}))
	assert.Equal(t, bulkString("1"), zscoreCommand(store, []string{"k", "b"}))
	assert.Equal(t, nilBulk, zscoreCommand(store, []string{"k", "nosuch"}))

	// Rank is ascending by score: b(1) < c(2) < a(3).
	assert.Equal(t, integer(0), zrankCommand(store, []string{"k", "b"}))
	assert.Equal(t, integer(1), zrankCommand(store, []string{"k", "c"}))
	assert.Equal(t, integer(2), zrankCommand(store, []string{"k", "a"}))

	// Whole-number scores render without a trailing ".0".
	assert.Equal(t, bulkString("3"), zscoreCommand(store, []string{"k", "a"}))
}

func TestZincrby(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, bulkString("5"), zincrbyCommand(store, []string{"k", "5", "a"}))
	assert.Equal(t, bulkString("8"), zincrbyCommand(store, []string{"k", "3", "a"}))
	assert.Equal(t, bulkString("-1.5"), zincrbyCommand(store, []string{"k", "-1.5", "b"}))
}

func TestZrem(t *testing.T) {
	store := NewDatastore()
	zaddCommand(store, []string{"k", "1", "a", "2", "b"})

	assert.Equal(t, integer(1), zremCommand(store, []string{"k", "a", "nosuch"}))
	assert.Equal(t, integer(1), zcardCommand(store, []string{"k"}))

	assert.Equal(t, integer(1), zremCommand(store, []string{"k", "b"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
}

func TestZrange(t *testing.T) {
	store := NewDatastore()
	zaddCommand(store, []string{"k", "2", "b", "1", "a", "1", "aa"}) // tie on score 1: a < aa lexically

	assert.Equal(t, arrayReply(bulkString("a"), bulkString("aa"), bulkString("b")),
		zrangeCommand(store, []string{"k", "0", "-1"}))

	assert.Equal(t, arrayReply(bulkString("b")), zrangeCommand(store, []string{"k", "-1", "-1"}))

	assert.Equal(t, arrayReply(
		bulkString("a"), bulkString("1"),
		bulkString("aa"), bulkString("1"),
	), zrangeCommand(store, []string{"k", "0", "1", "WITHSCORES"}))

	assert.Equal(t, emptyArray, zrangeCommand(store, []string{"missing", "0", "-1"}))
}

func TestZrangebyscoreAndZcount(t *testing.T) {
	store := NewDatastore()
	zaddCommand(store, []string{"k", "1", "a", "2", "b", "3", "c", "4", "d"})

	assert.Equal(t, arrayReply(bulkString("b"), bulkString("c")),
		zrangebyscoreCommand(store, []string{"k", "2", "3"}))
	assert.Equal(t, integer(2), zcountCommand(store, []string{"k", "2", "3"}))

	assert.Equal(t, arrayReply(bulkString("a"), bulkString("b"), bulkString("c"), bulkString("d")),
		zrangebyscoreCommand(store, []string{"k", "-inf", "+inf"}))
	assert.Equal(t, integer(4), zcountCommand(store, []string{"k", "-inf", "+inf"}))

	// Exclusive bounds via "(score".
	assert.Equal(t, arrayReply(bulkString("c")),
		zrangebyscoreCommand(store, []string{"k", "(2", "(4"}))
	assert.Equal(t, integer(1), zcountCommand(store, []string{"k", "(2", "(4"}))

	assert.Equal(t, arrayReply(bulkString("b")),
		zrangebyscoreCommand(store, []string{"k", "1", "4", "LIMIT", "1", "1"}))

	assert.Equal(t, arrayReply(bulkString("a"), bulkString("1")),
		zrangebyscoreCommand(store, []string{"k", "1", "1", "WITHSCORES"}))

	assert.Equal(t, errMinMaxNotFloat, zrangebyscoreCommand(store, []string{"k", "notanumber", "3"}))
	assert.Equal(t, errMinMaxNotFloat, zcountCommand(store, []string{"k", "1", "notanumber"}))

	assert.Equal(t, emptyArray, zrangebyscoreCommand(store, []string{"missing", "0", "10"}))
	assert.Equal(t, integer(0), zcountCommand(store, []string{"missing", "0", "10"}))
}

func TestGetsetGetdel(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"k", "old"})

	assert.Equal(t, bulkString("old"), getsetCommand(store, []string{"k", "new"}))
	assert.Equal(t, bulkString("new"), getCommand(store, []string{"k"}))
	// GETSET on an absent key still creates it (that's the "set" half of
	// GETSET) — it's "missing2" that stays untouched for the checks below.
	assert.Equal(t, nilBulk, getsetCommand(store, []string{"missing1", "v"}))
	assert.Equal(t, bulkString("v"), getCommand(store, []string{"missing1"}))

	assert.Equal(t, bulkString("new"), getdelCommand(store, []string{"k"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	assert.Equal(t, nilBulk, getdelCommand(store, []string{"missing2"}))
}

func TestAppendStrlen(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, integer(5), appendCommand(store, []string{"k", "hello"}))
	assert.Equal(t, integer(11), appendCommand(store, []string{"k", " world"}))
	assert.Equal(t, bulkString("hello world"), getCommand(store, []string{"k"}))

	assert.Equal(t, integer(0), strlenCommand(store, []string{"missing"}))
	assert.Equal(t, integer(11), strlenCommand(store, []string{"k"}))
}

func TestSetnx(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer(1), setnxCommand(store, []string{"k", "v1"}))
	assert.Equal(t, integer(0), setnxCommand(store, []string{"k", "v2"}))
	assert.Equal(t, bulkString("v1"), getCommand(store, []string{"k"}))
}

func TestSetrange(t *testing.T) {
	t.Run("zero-pads a missing key", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(8), setrangeCommand(store, []string{"k", "5", "abc"}))
		v, _, _ := store.lookup("k", typeString)
		assert.Equal(t, "\x00\x00\x00\x00\x00abc", v.str)
	})

	t.Run("empty value against a missing key is a no-op", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(0), setrangeCommand(store, []string{"k", "0", ""}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	})

	t.Run("overwrites in place within bounds", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"k", "Hello World"})
		assert.Equal(t, integer(11), setrangeCommand(store, []string{"k", "6", "Redis"}))
		assert.Equal(t, bulkString("Hello Redis"), getCommand(store, []string{"k"}))
	})

	t.Run("negative offset is rejected", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, "-ERR offset is out of range\r\n", setrangeCommand(store, []string{"k", "-1", "x"}))
	})
}

func TestGetrange(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"k", "This is a string"})

	assert.Equal(t, bulkString("This"), getrangeCommand(store, []string{"k", "0", "3"}))
	assert.Equal(t, bulkString("ing"), getrangeCommand(store, []string{"k", "-3", "-1"}))
	assert.Equal(t, bulkString("This is a string"), getrangeCommand(store, []string{"k", "0", "-1"}))
	assert.Equal(t, bulkString(""), getrangeCommand(store, []string{"missing", "0", "-1"}))
}

func TestIncrDecrFamily(t *testing.T) {
	store := NewDatastore()

	assert.Equal(t, integer64(1), incrCommand(store, []string{"k"}))
	assert.Equal(t, integer64(0), decrCommand(store, []string{"k"}))
	assert.Equal(t, integer64(10), incrbyCommand(store, []string{"k", "10"}))
	assert.Equal(t, integer64(5), decrbyCommand(store, []string{"k", "5"}))

	setCommand(store, []string{"notanumber", "abc"})
	assert.Equal(t, errNotInteger, incrCommand(store, []string{"notanumber"}))

	setCommand(store, []string{"maxed", strconv.FormatInt(math.MaxInt64, 10)})
	assert.Equal(t, errOverflow, incrCommand(store, []string{"maxed"}))

	setCommand(store, []string{"minned", strconv.FormatInt(math.MinInt64, 10)})
	assert.Equal(t, errOverflow, decrCommand(store, []string{"minned"}))
	assert.Equal(t, errOverflow, decrbyCommand(store, []string{"minned", strconv.FormatInt(math.MinInt64, 10)}))
}

func TestIncrbyfloat(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, bulkString("10.5"), incrbyfloatCommand(store, []string{"k", "10.5"}))
	assert.Equal(t, bulkString("10"), incrbyfloatCommand(store, []string{"k", "-0.5"}))

	setCommand(store, []string{"notafloat", "abc"})
	assert.Equal(t, errNotFloat, incrbyfloatCommand(store, []string{"notafloat", "1"}))
	assert.Equal(t, errNotFloat, incrbyfloatCommand(store, []string{"k", "notafloat"}))
}

func TestMsetMsetnxMget(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, okReply, msetCommand(store, []string{"a", "1", "b", "2"}))
	assert.Equal(t, arrayReply(bulkString("1"), bulkString("2")), mgetCommand(store, []string{"a", "b"}))

	assert.Equal(t, wrongArgs("mset"), msetCommand(store, []string{"a"}))

	t.Run("MSETNX is all-or-nothing", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"exists", "old"})

		assert.Equal(t, integer(0), msetnxCommand(store, []string{"fresh1", "1", "exists", "2", "fresh2", "3"}))
		assert.Equal(t, arrayReply(nilBulk, bulkString("old"), nilBulk),
			mgetCommand(store, []string{"fresh1", "exists", "fresh2"}))

		assert.Equal(t, integer(1), msetnxCommand(store, []string{"fresh1", "1", "fresh2", "3"}))
	})

	t.Run("MGET treats a wrong-type key as absent, no WRONGTYPE", func(t *testing.T) {
		store := NewDatastore()
		hsetCommand(store, []string{"h", "f", "v"})
		setCommand(store, []string{"s", "v"})
		assert.Equal(t, arrayReply(nilBulk, bulkString("v")), mgetCommand(store, []string{"h", "s"}))
	})
}

func TestHexistsHlenHkeysHvalsHgetall(t *testing.T) {
	store := NewDatastore()
	hsetCommand(store, []string{"h", "f1", "v1"})
	hsetCommand(store, []string{"h", "f2", "v2"})

	assert.Equal(t, integer(1), hexistsCommand(store, []string{"h", "f1"}))
	assert.Equal(t, integer(0), hexistsCommand(store, []string{"h", "nosuch"}))
	assert.Equal(t, integer(0), hexistsCommand(store, []string{"missing", "f1"}))

	assert.Equal(t, integer(2), hlenCommand(store, []string{"h"}))
	assert.Equal(t, integer(0), hlenCommand(store, []string{"missing"}))

	keys := hkeysCommand(store, []string{"h"})
	assert.Contains(t, keys, bulkString("f1"))
	assert.Contains(t, keys, bulkString("f2"))
	assert.Equal(t, emptyArray, hkeysCommand(store, []string{"missing"}))

	vals := hvalsCommand(store, []string{"h"})
	assert.Contains(t, vals, bulkString("v1"))
	assert.Contains(t, vals, bulkString("v2"))

	all := hgetallCommand(store, []string{"h"})
	assert.Contains(t, all, bulkString("f1"))
	assert.Contains(t, all, bulkString("v1"))
	assert.Equal(t, emptyArray, hgetallCommand(store, []string{"missing"}))
}

func TestHmsetHmget(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, okReply, hmsetCommand(store, []string{"h", "f1", "v1", "f2", "v2"}))
	assert.Equal(t, bulkString("v1"), hgetCommand(store, []string{"h", "f1"}))

	assert.Equal(t, arrayReply(bulkString("v1"), nilBulk, bulkString("v2")),
		hmgetCommand(store, []string{"h", "f1", "nosuch", "f2"}))

	t.Run("WRONGTYPE errors the whole command, unlike MGET", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"s", "v"})
		assert.Equal(t, errWrongType, hmgetCommand(store, []string{"s", "f"}))
	})
}

func TestHsetnx(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer(1), hsetnxCommand(store, []string{"h", "f", "v1"}))
	assert.Equal(t, integer(0), hsetnxCommand(store, []string{"h", "f", "v2"}))
	assert.Equal(t, bulkString("v1"), hgetCommand(store, []string{"h", "f"}))
}

func TestHincrbyHincrbyfloatHstrlen(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer64(5), hincrbyCommand(store, []string{"h", "f", "5"}))
	assert.Equal(t, integer64(3), hincrbyCommand(store, []string{"h", "f", "-2"}))

	assert.Equal(t, bulkString("3.5"), hincrbyfloatCommand(store, []string{"h", "g", "3.5"}))

	assert.Equal(t, integer(1), hstrlenCommand(store, []string{"h", "f"}))
	assert.Equal(t, integer(0), hstrlenCommand(store, []string{"h", "nosuch"}))
	assert.Equal(t, integer(0), hstrlenCommand(store, []string{"missing", "f"}))
}

func TestHrandfield(t *testing.T) {
	store := NewDatastore()
	hsetCommand(store, []string{"h", "f1", "v1"})
	hsetCommand(store, []string{"h", "f2", "v2"})
	hsetCommand(store, []string{"h", "f3", "v3"})

	t.Run("no count returns one field or nil", func(t *testing.T) {
		reply := hrandfieldCommand(store, []string{"h"})
		assert.True(t, strings.HasPrefix(reply, "$"))
		assert.Equal(t, nilBulk, hrandfieldCommand(store, []string{"missing"}))
	})

	t.Run("positive count returns distinct fields, clamped to available", func(t *testing.T) {
		reply := hrandfieldCommand(store, []string{"h", "10"})
		assert.Equal(t, "*3\r\n", reply[:len("*3\r\n")])
	})

	t.Run("negative count allows repeats, exact count", func(t *testing.T) {
		reply := hrandfieldCommand(store, []string{"h", "-5"})
		assert.Equal(t, "*5\r\n", reply[:len("*5\r\n")])
	})

	t.Run("WITHVALUES doubles the output", func(t *testing.T) {
		reply := hrandfieldCommand(store, []string{"h", "2", "WITHVALUES"})
		assert.Equal(t, "*4\r\n", reply[:len("*4\r\n")])
	})

	t.Run("bad WITHVALUES token is a syntax error", func(t *testing.T) {
		assert.Equal(t, errSyntax, hrandfieldCommand(store, []string{"h", "2", "BOGUS"}))
	})
}

func TestSmembersSismemberSmismemberScard(t *testing.T) {
	store := NewDatastore()
	saddCommand(store, []string{"s", "a", "b"})

	members := smembersCommand(store, []string{"s"})
	assert.Contains(t, members, bulkString("a"))
	assert.Contains(t, members, bulkString("b"))
	assert.Equal(t, emptyArray, smembersCommand(store, []string{"missing"}))

	assert.Equal(t, integer(1), sismemberCommand(store, []string{"s", "a"}))
	assert.Equal(t, integer(0), sismemberCommand(store, []string{"s", "nosuch"}))
	assert.Equal(t, integer(0), sismemberCommand(store, []string{"missing", "a"}))

	assert.Equal(t, arrayReply(integer(1), integer(0)), smismemberCommand(store, []string{"s", "a", "nosuch"}))

	assert.Equal(t, integer(2), scardCommand(store, []string{"s"}))
	assert.Equal(t, integer(0), scardCommand(store, []string{"missing"}))
}

func TestSpop(t *testing.T) {
	t.Run("single pop without count", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"s", "a"})
		assert.Equal(t, bulkString("a"), spopCommand(store, []string{"s"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"s"}))
		assert.Equal(t, nilBulk, spopCommand(store, []string{"missing"}))
	})

	t.Run("count on a missing key is emptyArray, not nilArray (contrast with LPOP)", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, emptyArray, spopCommand(store, []string{"missing", "2"}))
	})

	t.Run("count larger than the set is clamped and empties the key", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"s", "a", "b"})
		reply := spopCommand(store, []string{"s", "10"})
		assert.Equal(t, "*2\r\n", reply[:len("*2\r\n")])
		assert.Equal(t, noneReply, typeCommand(store, []string{"s"}))
	})

	t.Run("negative count is rejected", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"s", "a"})
		assert.Equal(t, errMustBePositive, spopCommand(store, []string{"s", "-1"}))
	})
}

func TestSrandmember(t *testing.T) {
	store := NewDatastore()
	saddCommand(store, []string{"s", "a", "b", "c"})

	assert.True(t, strings.HasPrefix(srandmemberCommand(store, []string{"s"}), "$"))
	assert.Equal(t, nilBulk, srandmemberCommand(store, []string{"missing"}))

	reply := srandmemberCommand(store, []string{"s", "10"})
	assert.Equal(t, "*3\r\n", reply[:len("*3\r\n")])

	reply = srandmemberCommand(store, []string{"s", "-5"})
	assert.Equal(t, "*5\r\n", reply[:len("*5\r\n")])

	// SRANDMEMBER never removes anything, unlike SPOP.
	assert.Equal(t, integer(3), scardCommand(store, []string{"s"}))
}

func TestSmove(t *testing.T) {
	t.Run("between two different keys", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"src", "a", "b"})

		assert.Equal(t, integer(1), smoveCommand(store, []string{"src", "dst", "a"}))
		assert.Equal(t, integer(0), sismemberCommand(store, []string{"src", "a"}))
		assert.Equal(t, integer(1), sismemberCommand(store, []string{"dst", "a"}))
	})

	t.Run("member not in source", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"src", "a"})
		assert.Equal(t, integer(0), smoveCommand(store, []string{"src", "dst", "nosuch"}))
	})

	t.Run("WRONGTYPE on destination leaves source untouched", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"src", "a"})
		setCommand(store, []string{"dst", "not-a-set"})

		assert.Equal(t, errWrongType, smoveCommand(store, []string{"src", "dst", "a"}))
		assert.Equal(t, integer(1), sismemberCommand(store, []string{"src", "a"}))
	})
}

func TestSunionSinterSdiff(t *testing.T) {
	store := NewDatastore()
	saddCommand(store, []string{"a", "1", "2", "3"})
	saddCommand(store, []string{"b", "2", "3", "4"})

	union := sunionCommand(store, []string{"a", "b"})
	for _, m := range []string{"1", "2", "3", "4"} {
		assert.Contains(t, union, bulkString(m))
	}

	inter := sinterCommand(store, []string{"a", "b"})
	assert.Equal(t, "*2\r\n", inter[:len("*2\r\n")])
	assert.Contains(t, inter, bulkString("2"))
	assert.Contains(t, inter, bulkString("3"))

	assert.Equal(t, arrayReply(bulkString("1")), sdiffCommand(store, []string{"a", "b"}))

	t.Run("an absent input key is treated as an empty set", func(t *testing.T) {
		assert.Equal(t, emptyArray, sinterCommand(store, []string{"a", "missing"}))
		union := sunionCommand(store, []string{"a", "missing"})
		for _, m := range []string{"1", "2", "3"} {
			assert.Contains(t, union, bulkString(m))
		}
	})

	t.Run("WRONGTYPE when an input key isn't a set", func(t *testing.T) {
		store := NewDatastore()
		saddCommand(store, []string{"a", "1"})
		setCommand(store, []string{"s", "x"})
		assert.Equal(t, errWrongType, sunionCommand(store, []string{"a", "s"}))
		assert.Equal(t, errWrongType, sinterCommand(store, []string{"a", "s"}))
		assert.Equal(t, errWrongType, sdiffCommand(store, []string{"a", "s"}))
	})
}

func TestSunionstoreSinterstoreSdiffstore(t *testing.T) {
	store := NewDatastore()
	saddCommand(store, []string{"a", "1", "2"})
	saddCommand(store, []string{"b", "2", "3"})

	assert.Equal(t, integer(3), sunionstoreCommand(store, []string{"dest", "a", "b"}))
	assert.Equal(t, integer(3), scardCommand(store, []string{"dest"}))

	assert.Equal(t, integer(1), sinterstoreCommand(store, []string{"dest", "a", "b"}))
	assert.Equal(t, integer(1), scardCommand(store, []string{"dest"}))

	assert.Equal(t, integer(1), sdiffstoreCommand(store, []string{"dest", "a", "b"}))

	t.Run("an empty result removes the destination key", func(t *testing.T) {
		saddCommand(store, []string{"x", "1"})
		saddCommand(store, []string{"y", "1"})
		assert.Equal(t, integer(0), sdiffstoreCommand(store, []string{"dest", "x", "y"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"dest"}))
	})
}

func TestSintercard(t *testing.T) {
	store := NewDatastore()
	saddCommand(store, []string{"a", "1", "2", "3"})
	saddCommand(store, []string{"b", "2", "3", "4"})

	assert.Equal(t, integer(2), sintercardCommand(store, []string{"2", "a", "b"}))
	assert.Equal(t, integer(1), sintercardCommand(store, []string{"2", "a", "b", "LIMIT", "1"}))
	assert.Equal(t, integer(2), sintercardCommand(store, []string{"2", "a", "b", "LIMIT", "0"})) // 0 = unlimited

	assert.Equal(t, errSyntax, sintercardCommand(store, []string{"2", "a", "b", "BOGUS", "1"}))
}

func TestExistsUnlink(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"k", "v"})

	assert.Equal(t, integer(2), existsCommand(store, []string{"k", "k"}))
	assert.Equal(t, integer(1), existsCommand(store, []string{"k", "missing"}))

	assert.Equal(t, integer(1), unlinkCommand(store, []string{"k"}))
	assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
	assert.Equal(t, integer(0), unlinkCommand(store, []string{"missing"}))
}

func TestKeys(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"foo", "1"})
	setCommand(store, []string{"foobar", "1"})
	setCommand(store, []string{"baz", "1"})

	reply := keysCommand(store, []string{"foo*"})
	assert.Contains(t, reply, bulkString("foo"))
	assert.Contains(t, reply, bulkString("foobar"))
	assert.NotContains(t, reply, bulkString("baz"))

	assert.Equal(t, emptyArray, keysCommand(store, []string{"nomatch*"}))
}

func TestRenameRenamenx(t *testing.T) {
	t.Run("moves the value and preserves TTL", func(t *testing.T) {
		now := time.Now()
		store := clockAt(now)
		setCommand(store, []string{"k", "v", "EX", "100"})

		assert.Equal(t, okReply, renameCommand(store, []string{"k", "k2"}))
		assert.Equal(t, noneReply, typeCommand(store, []string{"k"}))
		assert.Equal(t, bulkString("v"), getCommand(store, []string{"k2"}))
		assert.Equal(t, integer(100), ttlCommand(store, []string{"k2"}))
	})

	t.Run("renaming a key to itself is a no-op success", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"k", "v"})
		assert.Equal(t, okReply, renameCommand(store, []string{"k", "k"}))
		assert.Equal(t, bulkString("v"), getCommand(store, []string{"k"}))
	})

	t.Run("absent source errors", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, errNoSuchKey, renameCommand(store, []string{"missing", "k2"}))
	})

	t.Run("RENAME overwrites an existing destination", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"k", "v1"})
		setCommand(store, []string{"k2", "v2"})
		assert.Equal(t, okReply, renameCommand(store, []string{"k", "k2"}))
		assert.Equal(t, bulkString("v1"), getCommand(store, []string{"k2"}))
	})

	t.Run("RENAMENX refuses an existing destination", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"k", "v1"})
		setCommand(store, []string{"k2", "v2"})
		assert.Equal(t, integer(0), renamenxCommand(store, []string{"k", "k2"}))
		assert.Equal(t, bulkString("v1"), getCommand(store, []string{"k"}))
		assert.Equal(t, bulkString("v2"), getCommand(store, []string{"k2"}))
	})

	t.Run("RENAMENX succeeds against a fresh destination", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"k", "v1"})
		assert.Equal(t, integer(1), renamenxCommand(store, []string{"k", "k2"}))
		assert.Equal(t, bulkString("v1"), getCommand(store, []string{"k2"}))
	})
}

func TestRandomkeyDbsize(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, nilBulk, randomkeyCommand(store, nil))
	assert.Equal(t, integer(0), dbsizeCommand(store, nil))

	setCommand(store, []string{"a", "1"})
	setCommand(store, []string{"b", "2"})
	assert.Equal(t, integer(2), dbsizeCommand(store, nil))

	reply := randomkeyCommand(store, nil)
	assert.True(t, reply == bulkString("a") || reply == bulkString("b"))
}

func TestFlushdbFlushall(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"a", "1"})
	hsetCommand(store, []string{"b", "f", "v"})

	assert.Equal(t, okReply, flushdbCommand(store, nil))
	assert.Empty(t, store.keys)
	assert.Contains(t, store.stale, stringPrefix+"a")
	assert.Contains(t, store.stale, hashPrefix+"b")

	store2 := NewDatastore()
	setCommand(store2, []string{"a", "1"})
	assert.Equal(t, okReply, flushallCommand(store2, []string{"ASYNC"}))
	assert.Empty(t, store2.keys)

	assert.Equal(t, errSyntax, flushdbCommand(store, []string{"BOGUS"}))
}

func TestCopy(t *testing.T) {
	t.Run("produces an independent copy", func(t *testing.T) {
		store := NewDatastore()
		hsetCommand(store, []string{"src", "f", "v1"})

		assert.Equal(t, integer(1), copyCommand(store, []string{"src", "dst"}))
		hsetCommand(store, []string{"dst", "f", "changed"})

		assert.Equal(t, bulkString("v1"), hgetCommand(store, []string{"src", "f"}))
		assert.Equal(t, bulkString("changed"), hgetCommand(store, []string{"dst", "f"}))
	})

	t.Run("fails without REPLACE against an existing destination", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"src", "v1"})
		setCommand(store, []string{"dst", "v2"})
		assert.Equal(t, integer(0), copyCommand(store, []string{"src", "dst"}))
		assert.Equal(t, bulkString("v2"), getCommand(store, []string{"dst"}))
	})

	t.Run("REPLACE overwrites an existing destination", func(t *testing.T) {
		store := NewDatastore()
		setCommand(store, []string{"src", "v1"})
		setCommand(store, []string{"dst", "v2"})
		assert.Equal(t, integer(1), copyCommand(store, []string{"src", "dst", "REPLACE"}))
		assert.Equal(t, bulkString("v1"), getCommand(store, []string{"dst"}))
	})

	t.Run("copies the TTL too", func(t *testing.T) {
		now := time.Now()
		store := clockAt(now)
		setCommand(store, []string{"src", "v", "EX", "50"})
		copyCommand(store, []string{"src", "dst"})
		assert.Equal(t, integer(50), ttlCommand(store, []string{"dst"}))
	})

	t.Run("absent source", func(t *testing.T) {
		store := NewDatastore()
		assert.Equal(t, integer(0), copyCommand(store, []string{"missing", "dst"}))
	})
}

func TestEcho(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, bulkString("hello"), echoCommand(store, []string{"hello"}))
	assert.Equal(t, wrongArgs("echo"), echoCommand(store, nil))
}

func TestTime(t *testing.T) {
	// 6000ns = 6us, so the reply's microsecond field should read "6".
	now := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	store := clockAt(now)

	assert.Equal(t, arrayReply(
		bulkString(strconv.FormatInt(now.Unix(), 10)),
		bulkString("6"),
	), timeCommand(store, nil))
}

func TestLastsave(t *testing.T) {
	store := NewDatastore()
	assert.Equal(t, integer64(0), lastsaveCommand(store, nil))
	store.lastSaveUnix = 12345
	assert.Equal(t, integer64(12345), lastsaveCommand(store, nil))
}

func TestInfo(t *testing.T) {
	store := NewDatastore()
	setCommand(store, []string{"k", "v"})

	reply := infoCommand(store, nil)
	assert.True(t, strings.HasPrefix(reply, "$"))
	assert.Contains(t, reply, "cachekv_version:")
	assert.Contains(t, reply, "db0:keys=1")

	// section is accepted but ignored — same reply regardless.
	assert.Equal(t, reply, infoCommand(store, []string{"keyspace"}))
}

func TestDebugSleep(t *testing.T) {
	store := NewDatastore()
	start := time.Now()
	assert.Equal(t, okReply, debugCommand(store, []string{"SLEEP", "0.05"}))
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)

	assert.Contains(t, debugCommand(store, []string{"BOGUS"}), "-ERR")
}

// TestBgsaveForcesRegardlessOfDirty proves BGSAVE forces a save attempt even
// when dirty is below saveMinChanges — unlike a plain SAVE, which skips the
// attempt entirely. Neither test has a real cachekv database to save to, but
// that asymmetry (skip vs. attempt-and-fail) is exactly what's observable
// without one: SAVE replies +OK having done nothing, BGSAVE replies the save
// error having actually tried.
func TestBgsaveForcesRegardlessOfDirty(t *testing.T) {
	store := NewDatastore()

	cmdChan := make(chan CkvCommand)
	go stateProcessor(cmdChan, store, "no-such-persist-db", 1<<30) // dirty (0) never reaches this
	defer close(cmdChan)

	saveResp := make(chan string, 1)
	cmdChan <- CkvCommand{Op: "SAVE", Resp: saveResp}
	assert.Equal(t, okReply, <-saveResp) // skipped: dirty (0) < saveMinChanges

	bgResp := make(chan string, 1)
	cmdChan <- CkvCommand{Op: "BGSAVE", Resp: bgResp}
	assert.True(t, strings.HasPrefix(<-bgResp, "-ERR save failed")) // forced: actually attempted, and failed
}
