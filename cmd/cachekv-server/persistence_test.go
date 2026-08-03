package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestReconcileThreeWayCollisionPrefersString(t *testing.T) {
	entries := map[string][]byte{
		stringPrefix + "x": []byte("winner"),
		hashPrefix + "x":   mustJSON(t, map[string]string{"f": "v"}),
		setPrefix + "x":    mustJSON(t, []string{"m"}),
	}

	store, losers, skipped, _ := reconcile(entries)

	require.Contains(t, store.keys, "x")
	assert.Equal(t, typeString, store.keys["x"].kind)
	assert.Equal(t, "winner", store.keys["x"].str)
	assert.Equal(t, []string{hashPrefix + "x", setPrefix + "x"}, losers)
	assert.Empty(t, skipped)
}

func TestReconcileIsDeterministic(t *testing.T) {
	entries := map[string][]byte{
		stringPrefix + "x": []byte("winner"),
		hashPrefix + "x":   mustJSON(t, map[string]string{"f": "v"}),
		setPrefix + "x":    mustJSON(t, []string{"m"}),
	}

	_, wantLosers, wantSkipped, _ := reconcile(entries)
	for i := 0; i < 20; i++ {
		store, losers, skipped, _ := reconcile(entries)
		assert.Equal(t, typeString, store.keys["x"].kind)
		assert.Equal(t, wantLosers, losers)
		assert.Equal(t, wantSkipped, skipped)
	}
}

func TestReconcileHashOverSetCollision(t *testing.T) {
	entries := map[string][]byte{
		hashPrefix + "x": mustJSON(t, map[string]string{"f": "v"}),
		setPrefix + "x":  mustJSON(t, []string{"m"}),
	}

	store, losers, skipped, _ := reconcile(entries)

	require.Contains(t, store.keys, "x")
	assert.Equal(t, typeHash, store.keys["x"].kind)
	assert.Equal(t, []string{setPrefix + "x"}, losers)
	assert.Empty(t, skipped)
}

func TestReconcileSkipsUnknownPrefixAndUndecodableEntries(t *testing.T) {
	entries := map[string][]byte{
		"q:foo":          []byte("bar"),      // unknown prefix
		hashPrefix + "y": []byte("not json"), // undecodable
		setPrefix + "z":  []byte("not json"), // undecodable
	}

	store, losers, skipped, _ := reconcile(entries)

	assert.Empty(t, store.keys)
	assert.Empty(t, losers)
	assert.Equal(t, []string{hashPrefix + "y", "q:foo", setPrefix + "z"}, skipped)
}

func TestReconcileNoCollisionKeepsSingleEntry(t *testing.T) {
	entries := map[string][]byte{
		stringPrefix + "a": []byte("v"),
		hashPrefix + "b":   mustJSON(t, map[string]string{"f": "v"}),
		setPrefix + "c":    mustJSON(t, []string{"m"}),
	}

	store, losers, skipped, _ := reconcile(entries)

	assert.Len(t, store.keys, 3)
	assert.Empty(t, losers)
	assert.Empty(t, skipped)
}

func TestDescribeCollisionsNamesWinnerAndDroppedSizesPerKey(t *testing.T) {
	entries := map[string][]byte{
		stringPrefix + "x": []byte("winner"),                         // 6 bytes
		hashPrefix + "x":   mustJSON(t, map[string]string{"f": "v"}), // 9 bytes
		setPrefix + "x":    mustJSON(t, []string{"m"}),               // 5 bytes
	}
	store, losers, _, _ := reconcile(entries)
	require.Equal(t, []string{hashPrefix + "x", setPrefix + "x"}, losers)

	lines := describeCollisions(store, entries, losers)

	require.Len(t, lines, 1)
	line := lines[0]
	assert.Contains(t, line, `key collision on "x"`)
	assert.Contains(t, line, "keeping string")
	assert.Contains(t, line, fmt.Sprintf("hash (%d bytes)", len(entries[hashPrefix+"x"])))
	assert.Contains(t, line, fmt.Sprintf("set (%d bytes)", len(entries[setPrefix+"x"])))
}

func TestDescribeCollisionsOneLinePerCollidingKey(t *testing.T) {
	entries := map[string][]byte{
		stringPrefix + "a": []byte("v1"),
		hashPrefix + "a":   mustJSON(t, map[string]string{"f": "v"}),
		stringPrefix + "b": []byte("v2"),
		setPrefix + "b":    mustJSON(t, []string{"m"}),
	}
	store, losers, _, _ := reconcile(entries)

	lines := describeCollisions(store, entries, losers)

	assert.Len(t, lines, 2)
	assert.Contains(t, lines[0], `"a"`)
	assert.Contains(t, lines[1], `"b"`)
}

func TestDescribeCollisionsEmptyWhenNoLosers(t *testing.T) {
	store := NewDatastore()
	assert.Empty(t, describeCollisions(store, nil, nil))
}

func TestSnapshotReconcileRoundTripsTTL(t *testing.T) {
	store := NewDatastore()
	store.keys["k"] = &value{kind: typeString, str: "v", expireAt: 1234567890123}
	store.keys["nottl"] = &value{kind: typeString, str: "v2"}

	entries := snapshot(store)
	assert.Contains(t, entries, expirePrefix+"k")
	assert.NotContains(t, entries, expirePrefix+"nottl")

	reconciled, losers, skipped, orphanedExpiry := reconcile(entries)
	require.Contains(t, reconciled.keys, "k")
	assert.Equal(t, int64(1234567890123), reconciled.keys["k"].expireAt)
	assert.Zero(t, reconciled.keys["nottl"].expireAt)
	assert.Empty(t, losers)
	assert.Empty(t, skipped)
	assert.Empty(t, orphanedExpiry)
}

func TestReconcileOrphanedExpiryEntryHasNoLiveKey(t *testing.T) {
	entries := map[string][]byte{
		expirePrefix + "gone": []byte("1234567890123"),
	}

	store, losers, skipped, orphanedExpiry := reconcile(entries)

	assert.Empty(t, store.keys)
	assert.Empty(t, losers)
	assert.Empty(t, skipped)
	assert.Equal(t, []string{expirePrefix + "gone"}, orphanedExpiry)

	// Confirm this never reaches describeCollisions's store.keys[name].kind
	// dereference the way a type-collision loser would.
	assert.NotPanics(t, func() { describeCollisions(store, entries, losers) })
}

func TestReconcileUnparseableExpiryEntryIsSkipped(t *testing.T) {
	entries := map[string][]byte{
		expirePrefix + "k": []byte("not a number"),
	}

	store, losers, skipped, orphanedExpiry := reconcile(entries)

	assert.Empty(t, store.keys)
	assert.Empty(t, losers)
	assert.Empty(t, orphanedExpiry)
	assert.Equal(t, []string{expirePrefix + "k"}, skipped)
}

func TestSnapshotReconcileRoundTripsList(t *testing.T) {
	store := NewDatastore()
	store.keys["k"] = &value{kind: typeList, list: []string{"c", "b", "a"}}

	entries := snapshot(store)
	assert.Contains(t, entries, listPrefix+"k")

	reconciled, losers, skipped, _ := reconcile(entries)
	require.Contains(t, reconciled.keys, "k")
	assert.Equal(t, typeList, reconciled.keys["k"].kind)
	assert.Equal(t, []string{"c", "b", "a"}, reconciled.keys["k"].list)
	assert.Empty(t, losers)
	assert.Empty(t, skipped)
}

func TestSnapshotReconcileRoundTripsZSet(t *testing.T) {
	store := NewDatastore()
	store.keys["k"] = &value{kind: typeZSet, zset: map[string]float64{"a": 1.5, "b": 2}}

	entries := snapshot(store)
	assert.Contains(t, entries, zsetPrefix+"k")

	reconciled, losers, skipped, _ := reconcile(entries)
	require.Contains(t, reconciled.keys, "k")
	assert.Equal(t, typeZSet, reconciled.keys["k"].kind)
	assert.Equal(t, map[string]float64{"a": 1.5, "b": 2}, reconciled.keys["k"].zset)
	assert.Empty(t, losers)
	assert.Empty(t, skipped)
}

func TestReconcileSkipsUndecodableListAndZSetEntries(t *testing.T) {
	entries := map[string][]byte{
		listPrefix + "l": []byte("not json"),
		zsetPrefix + "z": []byte("not json"),
	}

	store, losers, skipped, _ := reconcile(entries)

	assert.Empty(t, store.keys)
	assert.Empty(t, losers)
	assert.Equal(t, []string{listPrefix + "l", zsetPrefix + "z"}, skipped)
}

func TestTypeFromPrefixRecognizesListAndZSet(t *testing.T) {
	assert.Equal(t, typeList, typeFromPrefix(listPrefix))
	assert.Equal(t, typeZSet, typeFromPrefix(zsetPrefix))
}
