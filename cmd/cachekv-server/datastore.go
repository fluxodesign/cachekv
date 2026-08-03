package main

import "fmt"

type valueType uint8

const (
	typeString valueType = iota
	typeHash
	typeSet
)

// String is the reply for the TYPE command and the wording used in collision
// logs.
func (t valueType) String() string {
	switch t {
	case typeString:
		return "string"
	case typeHash:
		return "hash"
	case typeSet:
		return "set"
	default:
		return "unknown"
	}
}

// prefix is the persisted-key prefix for this type (see persistence.go)
func (t valueType) prefix() string {
	switch t {
	case typeString:
		return stringPrefix
	case typeHash:
		return hashPrefix
	case typeSet:
		return setPrefix
	default:
		return ""
	}
}

// value is one entry in the keyspace: a type tag plus exactly one populated
// payload, chosen by that tag. The other payloads are always nil/zero.
type value struct {
	kind valueType
	str  string
	hash map[string]string
	set  map[string]struct{}
}

// newEmptyValue builds a zero-value payload of kind, with the payload map
// initialised so handlers can write into it directly.
func newEmptyValue(kind valueType) *value {
	v := &value{kind: kind}
	switch kind {
	case typeHash:
		v.hash = make(map[string]string)
	case typeSet:
		v.set = make(map[string]struct{})
	}
	return v
}

type CkvCommand struct {
	Op   string
	Args []string
	Resp chan string
}

// writeOps marks the commands that dirty the store, for the save-min-changes
// threshold below (mirrors Redis's "save <sec> <changes>" directives).
var writeOps = map[string]bool{
	"SET":  true,
	"HSET": true,
	"SADD": true,
	"DEL":  true,
	"HDEL": true,
	"SREM": true,
}

// stateProcessor is the single goroutine allowed to touch store; commands
// (including the internal "SAVE" pseudo-command) all funnel through
// cmdChannel so no locking is ever needed around the maps.
func stateProcessor(cmdChannel <-chan CkvCommand, store *Datastore, persistDbName string, saveMinChanges int) {
	dirty := 0

	for cmd := range cmdChannel {
		if cmd.Op == "SAVE" {
			forced := len(cmd.Args) > 0 && cmd.Args[0] == "force"
			if forced || dirty >= saveMinChanges {
				err := saveToDisk(persistDbName, store)
				if err == nil {
					dirty = 0
				}
				if cmd.Resp != nil {
					if err != nil {
						cmd.Resp <- fmt.Sprintf("-ERR save failed: %v\r\n", err)
					} else {
						cmd.Resp <- okReply
					}
				}
			} else if cmd.Resp != nil {
				cmd.Resp <- okReply
			}
			continue
		}

		handler, exists := commandRegistry[cmd.Op]
		if !exists {
			cmd.Resp <- fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd.Op)
			continue
		}

		// Execute the commmand and send the response back
		response := handler(store, cmd.Args)
		if writeOps[cmd.Op] {
			dirty++
		}
		cmd.Resp <- response
	}
}

// Datastore is the unified keyspace: each key holds exactly one value of
// exactly one type, matching Redis semantics.
type Datastore struct {
	keys map[string]*value

	// stale holds persisted keys (already prefixed) whose in-memory value is
	// gone because the key changed type. saveToDisk removes them, since
	// BatchInsert only upserts. Not part of snapshot.
	stale map[string]struct{}
}

func NewDatastore() *Datastore {
	return &Datastore{
		keys:  make(map[string]*value),
		stale: make(map[string]struct{}),
	}
}

// lookup returns the value at key when it holds kind. found is false when the
// key is absent. errReply is a non-empty RESP error when the key exists with
// a different kind, in which case the caller returns it verbatim.
func (d *Datastore) lookup(key string, kind valueType) (v *value, found bool, errReply string) {
	v, found = d.keys[key]
	if !found {
		return nil, false, ""
	}
	if v.kind != kind {
		return nil, false, errWrongType
	}
	return v, true, ""
}

// mutable returns the value at key, creating an empty one of kind if absent.
// errReply is non-empty (WRONGTYPE) when the key exists with a different
// kind, in which case nothing is created.
func (d *Datastore) mutable(key string, kind valueType) (v *value, errReply string) {
	existing, found := d.keys[key]
	if found {
		if existing.kind != kind {
			return nil, errWrongType
		}
		return existing, ""
	}
	v = newEmptyValue(kind)
	d.keys[key] = v
	delete(d.stale, kind.prefix()+key)
	return v, ""
}

// replace stores v at key whatever was there before, recording the previous
// kind's persisted key as stale when the kind changed. This is SET's
// type-agnostic overwrite.
func (d *Datastore) replace(key string, v *value) {
	if old, found := d.keys[key]; found && old.kind != v.kind {
		d.stale[old.kind.prefix()+key] = struct{}{}
	}
	d.keys[key] = v
	delete(d.stale, v.kind.prefix()+key)
}

// remove deletes key from the keyspace whatever type it holds, recording its
// persisted name as stale so the next save drops it from disk too. Reports
// whether the key was there to begin with. Marking a key that was never
// persisted is harmless: the removal is a no-op delete on a name that isn't
// on disk.
func (d *Datastore) remove(key string) bool {
	v, found := d.keys[key]
	if !found {
		return false
	}
	delete(d.keys, key)
	d.stale[v.kind.prefix()+key] = struct{}{}
	return true
}

type CommandHandler func(store *Datastore, args []string) string

// command registry
var commandRegistry = map[string]CommandHandler{
	"PING": pingCommand,
	"GET":  getCommand,
	"SET":  setCommand,
	"HSET": hsetCommand,
	"HGET": hgetCommand,
	"SADD": saddCommand,
	"TYPE": typeCommand,
	"DEL":  delCommand,
	"HDEL": hdelCommand,
	"SREM": sremCommand,
}

func pingCommand(store *Datastore, args []string) string {
	if len(args) > 0 {
		return bulkString(args[0])
	}
	return pongReply
}

func setCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	store.replace(args[0], &value{kind: typeString, str: args[1]})
	return okReply
}

func getCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("get")
	}
	v, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	return bulkString(v.str)
}

func hsetCommand(store *Datastore, args []string) string {
	if len(args) < 3 {
		return wrongArgs("hset")
	}
	key, field, val := args[0], args[1], args[2]

	v, errReply := store.mutable(key, typeHash)
	if errReply != "" {
		return errReply
	}

	v.hash[field] = val
	return ":1\r\n" // Integer reply indicating 1 field was added
}

func hgetCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("hget")
	}
	key, field := args[0], args[1]

	v, found, errReply := store.lookup(key, typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	val, exists := v.hash[field]
	if !exists {
		return nilBulk
	}
	return bulkString(val)
}

// hdelCommand implements HDEL key field [field ...], replying with the number
// of fields that were actually present. As in Redis, a hash left with no
// fields stops existing: the key is removed from the keyspace so TYPE reports
// none and the persisted h: entry is dropped on the next save.
func hdelCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("hdel")
	}
	key, fields := args[0], args[1:]

	v, found, errReply := store.lookup(key, typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	removed := 0
	for _, field := range fields {
		if _, exists := v.hash[field]; exists {
			delete(v.hash, field)
			removed++
		}
	}
	if len(v.hash) == 0 {
		store.remove(key)
	}
	return integer(removed)
}

func typeCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("type")
	}
	v, found := store.keys[args[0]]
	if !found {
		return noneReply
	}
	return "+" + v.kind.String() + "\r\n"
}

// delCommand implements DEL key [key ...], replying with the number of keys
// that existed. Like Redis's DEL it is type-agnostic — no WRONGTYPE, whatever
// the key holds goes.
func delCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	removed := 0
	for _, key := range args {
		if store.remove(key) {
			removed++
		}
	}
	return integer(removed)
}

func saddCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("sadd")
	}
	key, members := args[0], args[1:]

	v, errReply := store.mutable(key, typeSet)
	if errReply != "" {
		return errReply
	}

	added := 0
	for _, member := range members {
		if _, exists := v.set[member]; !exists {
			v.set[member] = struct{}{}
			added++
		}
	}
	return integer(added) // Integer reply of members added
}

// sremCommand implements SREM key member [member ...], replying with the
// number of members that were actually present. As in Redis, an empty set
// stops existing: the key is removed from the keyspace so TYPE reports none
// and the persisted z: entry is dropped on the next save.
func sremCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("srem")
	}
	key, members := args[0], args[1:]

	v, found, errReply := store.lookup(key, typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	removed := 0
	for _, member := range members {
		if _, exists := v.set[member]; exists {
			delete(v.set, member)
			removed++
		}
	}
	if len(v.set) == 0 {
		store.remove(key)
	}
	return integer(removed)
}
