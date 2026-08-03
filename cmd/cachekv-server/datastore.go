package main

import (
	"fmt"
	"math"
	mathrand "math/rand"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type valueType uint8

const (
	typeString valueType = iota
	typeHash
	typeSet
	typeList
	typeZSet
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
	case typeList:
		return "list"
	case typeZSet:
		return "zset"
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
	case typeList:
		return listPrefix
	case typeZSet:
		return zsetPrefix
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
	list []string
	zset map[string]float64

	// expireAt is a unix-epoch millisecond deadline; 0 means no TTL. Living
	// on value itself (rather than a second map keyed alongside d.keys)
	// means it's discarded "for free" whenever the value is: replace's
	// full-object swap and remove's delete(d.keys, key) already make the old
	// TTL unreachable, so only its persisted "e:" name needs explicit
	// staling (see remove/replace below).
	expireAt int64
}

// newEmptyValue builds a zero-value payload of kind, with the payload map
// initialised so handlers can write into it directly. list stays nil:
// append works fine on a nil slice, and a list only ever becomes non-empty
// via a push that immediately follows creation.
func newEmptyValue(kind valueType) *value {
	v := &value{kind: kind}
	switch kind {
	case typeHash:
		v.hash = make(map[string]string)
	case typeSet:
		v.set = make(map[string]struct{})
	case typeZSet:
		v.zset = make(map[string]float64)
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
	"SET":          true,
	"HSET":         true,
	"SADD":         true,
	"DEL":          true,
	"HDEL":         true,
	"SREM":         true,
	"EXPIRE":       true,
	"PEXPIRE":      true,
	"EXPIREAT":     true,
	"PEXPIREAT":    true,
	"PERSIST":      true,
	"SETEX":        true,
	"PSETEX":       true,
	"GETEX":        true,
	"LPUSH":        true,
	"RPUSH":        true,
	"LPOP":         true,
	"RPOP":         true,
	"LREM":         true,
	"LSET":         true,
	"LINSERT":      true,
	"LTRIM":        true,
	"LMOVE":        true,
	"ZADD":         true,
	"ZREM":         true,
	"ZINCRBY":      true,
	"GETSET":       true,
	"GETDEL":       true,
	"APPEND":       true,
	"SETNX":        true,
	"SETRANGE":     true,
	"INCR":         true,
	"DECR":         true,
	"INCRBY":       true,
	"DECRBY":       true,
	"INCRBYFLOAT":  true,
	"MSET":         true,
	"MSETNX":       true,
	"HMSET":        true,
	"HSETNX":       true,
	"HINCRBY":      true,
	"HINCRBYFLOAT": true,
	"SPOP":         true,
	"SMOVE":        true,
	"SUNIONSTORE":  true,
	"SINTERSTORE":  true,
	"SDIFFSTORE":   true,
	"UNLINK":       true,
	"RENAME":       true,
	"RENAMENX":     true,
	"FLUSHDB":      true,
	"FLUSHALL":     true,
	"COPY":         true,
}

// stateProcessor is the single goroutine allowed to touch store; commands
// (including the internal "SAVE" pseudo-command) all funnel through
// cmdChannel so no locking is ever needed around the maps.
func stateProcessor(cmdChannel <-chan CkvCommand, store *Datastore, persistDbName string, saveMinChanges int) {
	dirty := 0

	for cmd := range cmdChannel {
		if cmd.Op == "SAVE" || cmd.Op == "BGSAVE" {
			// BGSAVE always forces a save (no real background/fork — see
			// datastore.go's known-gaps note — but a client asking for one
			// shouldn't get skipped by the dirty threshold) and replies
			// Redis's own wording instead of a plain OK.
			forced := cmd.Op == "BGSAVE" || (len(cmd.Args) > 0 && cmd.Args[0] == "force")
			successReply := okReply
			if cmd.Op == "BGSAVE" {
				successReply = "+Background saving started\r\n"
			}
			if forced || dirty >= saveMinChanges {
				err := saveToDisk(persistDbName, store)
				if err == nil {
					dirty = 0
					store.lastSaveUnix = store.now().Unix()
				}
				if cmd.Resp != nil {
					if err != nil {
						cmd.Resp <- fmt.Sprintf("-ERR save failed: %v\r\n", err)
					} else {
						cmd.Resp <- successReply
					}
				}
			} else if cmd.Resp != nil {
				cmd.Resp <- successReply
			}
			continue
		}

		if cmd.Op == "EXPIRECYCLE" {
			// Passive expiration (via get()) guarantees nothing observable
			// is ever stale, but a TTL'd key nobody touches again would
			// otherwise sit in memory — and get re-persisted — forever.
			// Full scan, not Redis's own sampled active-expire cycle:
			// simpler and correct for a cache store's expected key counts.
			now := store.now().UnixMilli()
			removed := 0
			for key, v := range store.keys {
				if v.expireAt != 0 && now >= v.expireAt {
					store.remove(key)
					removed++
				}
			}
			dirty += removed
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

	// config backs CONFIG GET/SET. Server settings, not keyspace data — never
	// touched by snapshot/reconcile. Lives here rather than behind its own
	// lock because it's only ever mutated from stateProcessor, same as keys
	// and stale.
	config map[string]string

	// now is the clock every expiry computation reads instead of calling
	// time.Now() directly, so tests can set a fixed time instead of sleeping.
	now func() time.Time

	// lastSaveUnix is the unix timestamp of the last successful save
	// (SAVE/BGSAVE/periodic tick alike), read back by LASTSAVE. Zero until
	// the first successful save.
	lastSaveUnix int64

	// startedAt stamps roughly when the store came up, for INFO's uptime.
	startedAt time.Time
}

func NewDatastore() *Datastore {
	return &Datastore{
		keys:  make(map[string]*value),
		stale: make(map[string]struct{}),
		config: map[string]string{
			"maxmemory":  "0",
			"appendonly": "no",
			"save":       "",
		},
		now:       time.Now,
		startedAt: time.Now(),
	}
}

// get returns the value at key, lazily expiring it first if its TTL has
// passed — Redis's own passive-expiration model: an expired key behaves as
// absent from the moment it's checked, not from whenever a sweep reaches it.
// The one chokepoint lookup/mutable/typeCommand/delCommand and every TTL
// command goes through, so passive expiration is enforced in exactly one
// place rather than re-checked at every call site.
func (d *Datastore) get(key string) (v *value, found bool) {
	v, found = d.keys[key]
	if found && v.expireAt != 0 && d.now().UnixMilli() >= v.expireAt {
		d.remove(key)
		return nil, false
	}
	return v, found
}

// lookup returns the value at key when it holds kind. found is false when the
// key is absent (or was just lazily expired). errReply is a non-empty RESP
// error when the key exists with a different kind, in which case the caller
// returns it verbatim.
func (d *Datastore) lookup(key string, kind valueType) (v *value, found bool, errReply string) {
	v, found = d.get(key)
	if !found {
		return nil, false, ""
	}
	if v.kind != kind {
		return nil, false, errWrongType
	}
	return v, true, ""
}

// mutable returns the value at key, creating an empty one of kind if absent
// (or just lazily expired). errReply is non-empty (WRONGTYPE) when the key
// exists with a different kind, in which case nothing is created.
func (d *Datastore) mutable(key string, kind valueType) (v *value, errReply string) {
	existing, found := d.get(key)
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
// kind's persisted key as stale when the kind changed, and the previous
// expiry's persisted key as stale when the new value carries no TTL forward
// (plain SET always clears a prior TTL unless the caller copies it in via
// KEEPTTL). This is SET's type-agnostic overwrite.
func (d *Datastore) replace(key string, v *value) {
	if old, found := d.keys[key]; found {
		if old.kind != v.kind {
			d.stale[old.kind.prefix()+key] = struct{}{}
		}
		if old.expireAt != 0 && v.expireAt == 0 {
			d.stale[expirePrefix+key] = struct{}{}
		}
	}
	d.keys[key] = v
	delete(d.stale, v.kind.prefix()+key)
	if v.expireAt != 0 {
		delete(d.stale, expirePrefix+key)
	}
}

// remove deletes key from the keyspace whatever type it holds, recording its
// persisted name (and, if it carried a TTL, its persisted expiry name too) as
// stale so the next save drops both from disk. Reports whether the key was
// there to begin with. Marking a key that was never persisted is harmless:
// the removal is a no-op delete on a name that isn't on disk.
func (d *Datastore) remove(key string) bool {
	v, found := d.keys[key]
	if !found {
		return false
	}
	delete(d.keys, key)
	d.stale[v.kind.prefix()+key] = struct{}{}
	if v.expireAt != 0 {
		d.stale[expirePrefix+key] = struct{}{}
	}
	return true
}

// setExpireAt sets (ms > 0) or clears (ms == 0) key's TTL in place. Reports
// false if key doesn't exist (after lazy expiration) — EXPIRE/PERSIST/etc. on
// a missing key do nothing, matching Redis.
func (d *Datastore) setExpireAt(key string, ms int64) bool {
	v, found := d.get(key)
	if !found {
		return false
	}
	old := v.expireAt
	v.expireAt = ms
	switch {
	case ms == 0 && old != 0:
		d.stale[expirePrefix+key] = struct{}{}
	case ms != 0:
		delete(d.stale, expirePrefix+key)
	}
	return true
}

// ttlMillis returns the milliseconds remaining until key expires: -2 if it
// doesn't exist, -1 if it has no TTL, else the (non-negative) remainder.
func (d *Datastore) ttlMillis(key string) int64 {
	v, found := d.get(key)
	if !found {
		return -2
	}
	if v.expireAt == 0 {
		return -1
	}
	remaining := v.expireAt - d.now().UnixMilli()
	if remaining < 0 {
		remaining = 0 // get() would have lazily expired it otherwise
	}
	return remaining
}

type CommandHandler func(store *Datastore, args []string) string

// command registry
var commandRegistry = map[string]CommandHandler{
	"PING":          pingCommand,
	"GET":           getCommand,
	"SET":           setCommand,
	"HSET":          hsetCommand,
	"HGET":          hgetCommand,
	"SADD":          saddCommand,
	"TYPE":          typeCommand,
	"DEL":           delCommand,
	"HDEL":          hdelCommand,
	"SREM":          sremCommand,
	"SELECT":        selectCommand,
	"CONFIG":        configCommand,
	"EXPIRE":        expireCommand,
	"PEXPIRE":       pexpireCommand,
	"EXPIREAT":      expireatCommand,
	"PEXPIREAT":     pexpireatCommand,
	"TTL":           ttlCommand,
	"PTTL":          pttlCommand,
	"PERSIST":       persistCommand,
	"EXPIRETIME":    expiretimeCommand,
	"SETEX":         setexCommand,
	"PSETEX":        psetexCommand,
	"GETEX":         getexCommand,
	"LPUSH":         lpushCommand,
	"RPUSH":         rpushCommand,
	"LPOP":          lpopCommand,
	"RPOP":          rpopCommand,
	"LRANGE":        lrangeCommand,
	"LLEN":          llenCommand,
	"LREM":          lremCommand,
	"LSET":          lsetCommand,
	"LINSERT":       linsertCommand,
	"LTRIM":         ltrimCommand,
	"LMOVE":         lmoveCommand,
	"ZADD":          zaddCommand,
	"ZSCORE":        zscoreCommand,
	"ZRANGE":        zrangeCommand,
	"ZRANGEBYSCORE": zrangebyscoreCommand,
	"ZRANK":         zrankCommand,
	"ZREM":          zremCommand,
	"ZINCRBY":       zincrbyCommand,
	"ZCARD":         zcardCommand,
	"ZCOUNT":        zcountCommand,
	"GETSET":        getsetCommand,
	"GETDEL":        getdelCommand,
	"APPEND":        appendCommand,
	"STRLEN":        strlenCommand,
	"SETNX":         setnxCommand,
	"SETRANGE":      setrangeCommand,
	"GETRANGE":      getrangeCommand,
	"INCR":          incrCommand,
	"DECR":          decrCommand,
	"INCRBY":        incrbyCommand,
	"DECRBY":        decrbyCommand,
	"INCRBYFLOAT":   incrbyfloatCommand,
	"MSET":          msetCommand,
	"MSETNX":        msetnxCommand,
	"MGET":          mgetCommand,
	"HEXISTS":       hexistsCommand,
	"HLEN":          hlenCommand,
	"HKEYS":         hkeysCommand,
	"HVALS":         hvalsCommand,
	"HGETALL":       hgetallCommand,
	"HMSET":         hmsetCommand,
	"HMGET":         hmgetCommand,
	"HSETNX":        hsetnxCommand,
	"HINCRBY":       hincrbyCommand,
	"HINCRBYFLOAT":  hincrbyfloatCommand,
	"HSTRLEN":       hstrlenCommand,
	"HRANDFIELD":    hrandfieldCommand,
	"SMEMBERS":      smembersCommand,
	"SISMEMBER":     sismemberCommand,
	"SMISMEMBER":    smismemberCommand,
	"SCARD":         scardCommand,
	"SPOP":          spopCommand,
	"SRANDMEMBER":   srandmemberCommand,
	"SMOVE":         smoveCommand,
	"SUNION":        sunionCommand,
	"SINTER":        sinterCommand,
	"SDIFF":         sdiffCommand,
	"SUNIONSTORE":   sunionstoreCommand,
	"SINTERSTORE":   sinterstoreCommand,
	"SDIFFSTORE":    sdiffstoreCommand,
	"SINTERCARD":    sintercardCommand,
	"UNLINK":        unlinkCommand,
	"EXISTS":        existsCommand,
	"KEYS":          keysCommand,
	"RENAME":        renameCommand,
	"RENAMENX":      renamenxCommand,
	"RANDOMKEY":     randomkeyCommand,
	"DBSIZE":        dbsizeCommand,
	"FLUSHDB":       flushdbCommand,
	"FLUSHALL":      flushallCommand,
	"COPY":          copyCommand,
	"ECHO":          echoCommand,
	"TIME":          timeCommand,
	"LASTSAVE":      lastsaveCommand,
	"INFO":          infoCommand,
	"DEBUG":         debugCommand,
}

// COMMAND is registered here rather than in the commandRegistry literal
// above: commandCommand calls allCommandNames, which reads commandRegistry,
// and storing that reference directly in the literal makes the compiler see
// a variable-initialization cycle (commandRegistry -> commandCommand ->
// allCommandNames -> commandRegistry) even though nothing is actually called
// during initialization. init() runs after all package-level vars are set,
// so assigning here sidesteps that dependency graph entirely.
func init() {
	commandRegistry["COMMAND"] = commandCommand
}

func pingCommand(store *Datastore, args []string) string {
	if len(args) > 0 {
		return bulkString(args[0])
	}
	return pongReply
}

// setCommand implements SET key value [NX | XX] [EX sec | PX ms | EXAT ts |
// PXAT ts | KEEPTTL]. Redis's GET option is deliberately not implemented —
// it's orthogonal to expiry and not part of the bug this rewrite fixes (SET
// silently ignoring every option past the first two args). The plain 2-arg
// call is unaffected: no options means no TTL, matching prior behavior.
func setCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0], args[1]

	var (
		nx, xx, keepttl bool
		haveExpire      bool
		targetMs        int64
	)
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			if xx {
				return errSyntax
			}
			nx = true
		case "XX":
			if nx {
				return errSyntax
			}
			xx = true
		case "KEEPTTL":
			if haveExpire {
				return errSyntax
			}
			keepttl = true
		case "EX", "PX", "EXAT", "PXAT":
			if keepttl || haveExpire {
				return errSyntax
			}
			if i+1 >= len(args) {
				return errSyntax
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errNotInteger
			}
			switch strings.ToUpper(args[i]) {
			case "EX":
				if n <= 0 {
					return invalidExpireErr("set")
				}
				targetMs = store.now().UnixMilli() + n*1000
			case "PX":
				if n <= 0 {
					return invalidExpireErr("set")
				}
				targetMs = store.now().UnixMilli() + n
			case "EXAT":
				targetMs = n * 1000
			case "PXAT":
				targetMs = n
			}
			haveExpire = true
			i++
		default:
			return errSyntax
		}
	}

	existing, found := store.get(key)
	if nx && found {
		return nilBulk
	}
	if xx && !found {
		return nilBulk
	}

	newVal := &value{kind: typeString, str: val}
	switch {
	case keepttl && found:
		newVal.expireAt = existing.expireAt
	case haveExpire:
		newVal.expireAt = targetMs
	}

	// A target already in the past means the key must not exist afterward —
	// same as EXPIRE with a due/past deadline.
	if newVal.expireAt != 0 && newVal.expireAt <= store.now().UnixMilli() {
		store.remove(key) // in case it existed under a different kind/value
		return okReply
	}
	store.replace(key, newVal)
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
	v, found := store.get(args[0])
	if !found {
		return noneReply
	}
	return "+" + v.kind.String() + "\r\n"
}

// delCommand implements DEL key [key ...], replying with the number of keys
// that existed. Like Redis's DEL it is type-agnostic — no WRONGTYPE, whatever
// the key holds goes. Routed through get() first so a key that's logically
// expired but not yet swept reports as already-gone (0), not deleted (1).
func delCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	removed := 0
	for _, key := range args {
		if _, found := store.get(key); found {
			store.remove(key)
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

// selectCommand implements SELECT index. There is only one logical DB — the
// keyspace unification pass collapsed Strings/Hashes/Sets into one namespace
// — so anything but 0 is out of range, matching Redis's own wording for an
// unconfigured index.
func selectCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("select")
	}
	if args[0] != "0" {
		return "-ERR DB index is out of range\r\n"
	}
	return okReply
}

// configCommand implements CONFIG GET/SET over store.config's small known-key
// set. Anything else (bare CONFIG, an unknown subcommand) names the problem
// rather than guessing.
func configCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("config")
	}
	switch strings.ToUpper(args[0]) {
	case "GET":
		return configGet(store, args[1:])
	case "SET":
		return configSet(store, args[1:])
	default:
		return fmt.Sprintf("-ERR Unknown CONFIG subcommand or wrong number of arguments for '%s'\r\n", args[0])
	}
}

// configGet matches each known config key against every pattern (glob
// semantics via path.Match, close enough to Redis's own), returning a
// deduped, key-sorted array of alternating key/value bulk strings.
func configGet(store *Datastore, patterns []string) string {
	if len(patterns) < 1 {
		return wrongArgs("config|get")
	}
	keys := make([]string, 0, len(store.config))
	for key := range store.config {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	seen := make(map[string]bool, len(keys))
	var elems []string
	for _, key := range keys {
		for _, pattern := range patterns {
			if matched, err := path.Match(pattern, key); err == nil && matched {
				if !seen[key] {
					seen[key] = true
					elems = append(elems, bulkString(key), bulkString(store.config[key]))
				}
				break
			}
		}
	}
	return arrayReply(elems...)
}

// configSet sets exactly one known key to a new value. Redis 7's multi-pair
// form isn't supported — one pair is all any of the "worth doing first"
// clients actually send.
func configSet(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("config|set")
	}
	key := strings.ToLower(args[0])
	if _, known := store.config[key]; !known {
		return fmt.Sprintf("-ERR Unknown CONFIG parameter '%s'\r\n", args[0])
	}
	store.config[key] = args[1]
	return okReply
}

// allCommandNames lists every command the server accepts: commandRegistry's
// data-plane commands plus the connection-plane ones intercepted in
// handleConnection before ever reaching commandRegistry. Sorted because
// map iteration order is randomised and COMMAND's output should be stable.
func allCommandNames() []string {
	names := make([]string, 0, len(commandRegistry)+7)
	for name := range commandRegistry {
		names = append(names, name)
	}
	names = append(names, "AUTH", "HELLO", "QUIT", "CLIENT", "SHUTDOWN", "BGSAVE", "RESET")
	sort.Strings(names)
	return names
}

// commandCommand implements COMMAND / COMMAND COUNT / COMMAND DOCS. Arity and
// flags are placeholders — nothing in this codebase tracks real per-command
// metadata — and COMMAND DOCS is always empty; both are still valid RESP, so
// a client that merely probes on connect (e.g. redis-cli's COMMAND DOCS)
// stops hard-erroring, which is the actual bar this command needs to clear.
func commandCommand(store *Datastore, args []string) string {
	if len(args) == 0 {
		names := allCommandNames()
		entries := make([]string, len(names))
		for i, name := range names {
			entries[i] = arrayReply(bulkString(name), integer(-1), emptyArray, integer(0), integer(0), integer(0))
		}
		return arrayReply(entries...)
	}
	switch strings.ToUpper(args[0]) {
	case "COUNT":
		return integer(len(allCommandNames()))
	case "DOCS":
		return emptyArray
	default:
		return fmt.Sprintf("-ERR Unknown subcommand or wrong number of arguments for '%s'\r\n", args[0])
	}
}

// expireCondition reports whether newMs satisfies cond ("", "NX", "XX",
// "GT", "LT") against the key's current TTL, currentMs (0 = no TTL). A key
// with no TTL is "infinite" for GT/LT purposes, matching Redis: GT never
// fires against a no-TTL key, LT always does.
func expireCondition(cond string, currentMs, newMs int64) bool {
	switch cond {
	case "NX":
		return currentMs == 0
	case "XX":
		return currentMs != 0
	case "GT":
		return currentMs != 0 && newMs > currentMs
	case "LT":
		return currentMs == 0 || newMs < currentMs
	default:
		return true
	}
}

// expireGeneric implements EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT: unit converts
// args[1] to milliseconds, absolute selects whether args[1] is a duration
// (added to now) or an already-absolute deadline. An optional trailing
// NX/XX/GT/LT (Redis 7's conditional forms) gates whether the new deadline
// is actually applied. A deadline already due deletes the key immediately —
// matching Redis's own EXPIRE-with-a-past-time-is-DEL behavior.
func expireGeneric(store *Datastore, args []string, name string, unit time.Duration, absolute bool) string {
	if len(args) < 2 || len(args) > 3 {
		return wrongArgs(name)
	}
	n, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInteger
	}
	cond := ""
	if len(args) == 3 {
		cond = strings.ToUpper(args[2])
		if cond != "NX" && cond != "XX" && cond != "GT" && cond != "LT" {
			return fmt.Sprintf("-ERR Unsupported option %s\r\n", args[2])
		}
	}

	unitMs := int64(unit / time.Millisecond)
	var targetMs int64
	if absolute {
		targetMs = n * unitMs
	} else {
		targetMs = store.now().UnixMilli() + n*unitMs
	}

	v, found := store.get(args[0])
	if !found {
		return integer(0)
	}
	if !expireCondition(cond, v.expireAt, targetMs) {
		return integer(0)
	}
	if targetMs <= store.now().UnixMilli() {
		store.remove(args[0])
		return integer(1)
	}
	store.setExpireAt(args[0], targetMs)
	return integer(1)
}

func expireCommand(store *Datastore, args []string) string {
	return expireGeneric(store, args, "expire", time.Second, false)
}

func pexpireCommand(store *Datastore, args []string) string {
	return expireGeneric(store, args, "pexpire", time.Millisecond, false)
}

func expireatCommand(store *Datastore, args []string) string {
	return expireGeneric(store, args, "expireat", time.Second, true)
}

func pexpireatCommand(store *Datastore, args []string) string {
	return expireGeneric(store, args, "pexpireat", time.Millisecond, true)
}

// ttlCommand replies with seconds remaining, rounded to the nearest second
// ((ms+500)/1000, matching Redis's own rounding rather than truncating).
func ttlCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}
	ms := store.ttlMillis(args[0])
	if ms < 0 {
		return integer(int(ms))
	}
	return integer(int((ms + 500) / 1000))
}

func pttlCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("pttl")
	}
	return integer(int(store.ttlMillis(args[0])))
}

// persistCommand implements PERSIST key, clearing any TTL. Replies 1 if a
// TTL was actually removed, 0 otherwise (absent key or no TTL to begin with).
func persistCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("persist")
	}
	v, found := store.get(args[0])
	if !found || v.expireAt == 0 {
		return integer(0)
	}
	store.setExpireAt(args[0], 0)
	return integer(1)
}

// expiretimeCommand replies with the absolute unix-seconds deadline, or
// -1/-2 with the same meaning as TTL/PTTL.
func expiretimeCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("expiretime")
	}
	v, found := store.get(args[0])
	if !found {
		return integer(-2)
	}
	if v.expireAt == 0 {
		return integer(-1)
	}
	return integer(int(v.expireAt / 1000))
}

// setexGeneric implements SETEX/PSETEX: SET plus a mandatory positive TTL in
// one op. Rejects n <= 0 with Redis's own wording rather than silently
// accepting a nonsensical or immediately-expiring duration.
func setexGeneric(store *Datastore, args []string, name string, unit time.Duration) string {
	if len(args) != 3 {
		return wrongArgs(name)
	}
	n, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInteger
	}
	if n <= 0 {
		return invalidExpireErr(name)
	}
	targetMs := store.now().UnixMilli() + n*int64(unit/time.Millisecond)
	store.replace(args[0], &value{kind: typeString, str: args[2], expireAt: targetMs})
	return okReply
}

func setexCommand(store *Datastore, args []string) string {
	return setexGeneric(store, args, "setex", time.Second)
}

func psetexCommand(store *Datastore, args []string) string {
	return setexGeneric(store, args, "psetex", time.Millisecond)
}

// getexCommand implements GETEX key [EX sec | PX ms | EXAT ts | PXAT ts |
// PERSIST]. With no options it behaves exactly like GET — TTL untouched —
// which is easy to get backwards, since every other GETEX form exists
// specifically to mutate TTL.
func getexCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("getex")
	}
	key := args[0]
	v, found, errReply := store.lookup(key, typeString)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}

	if len(args) > 1 {
		switch strings.ToUpper(args[1]) {
		case "PERSIST":
			if len(args) != 2 {
				return wrongArgs("getex")
			}
			store.setExpireAt(key, 0)
		case "EX", "PX", "EXAT", "PXAT":
			if len(args) != 3 {
				return wrongArgs("getex")
			}
			n, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return errNotInteger
			}
			var targetMs int64
			switch strings.ToUpper(args[1]) {
			case "EX":
				if n <= 0 {
					return invalidExpireErr("getex")
				}
				targetMs = store.now().UnixMilli() + n*1000
			case "PX":
				if n <= 0 {
					return invalidExpireErr("getex")
				}
				targetMs = store.now().UnixMilli() + n
			case "EXAT":
				targetMs = n * 1000
			case "PXAT":
				targetMs = n
			}
			if targetMs <= store.now().UnixMilli() {
				store.remove(key)
				return bulkString(v.str) // the read already happened, matching Redis
			}
			store.setExpireAt(key, targetMs)
		default:
			return errSyntax
		}
	}
	return bulkString(v.str)
}

// normalizeRange applies Redis's LRANGE/ZRANGE/LTRIM index rules: negative
// indices count from the end, bounds clamp into range. Returns start > stop
// when there's nothing to return (an empty collection, or a range that
// clamps to nothing) — callers check that and reply however their command's
// "nothing here" case works (empty array, OK-and-remove-the-key, etc.).
func normalizeRange(start, stop, length int) (int, int) {
	if length == 0 {
		return 0, -1
	}
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += length
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, -1
	}
	return start, stop
}

// zmember is one sortedZMembers entry.
type zmember struct {
	member string
	score  float64
}

// sortedZMembers returns key's members ordered by score ascending, ties
// broken by member name ascending — Redis's own sorted-set ordering rule.
// Recomputed per call rather than cached in a maintained ordering; fine at
// this codebase's scale, the same trade-off already accepted by deferring
// SCAN's stable-cursor problem rather than half-solving it here.
func sortedZMembers(zset map[string]float64) []zmember {
	members := make([]zmember, 0, len(zset))
	for m, s := range zset {
		members = append(members, zmember{member: m, score: s})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].score != members[j].score {
			return members[i].score < members[j].score
		}
		return members[i].member < members[j].member
	})
	return members
}

// parseScoreRange parses a ZRANGEBYSCORE/ZCOUNT-style bound: "-inf", "+inf"
// (or "inf"), "(score" (exclusive), or "score" (inclusive) — Redis's actual
// bound syntax for both commands.
func parseScoreRange(s string) (score float64, exclusive bool, err error) {
	switch s {
	case "-inf":
		return math.Inf(-1), false, nil
	case "+inf", "inf":
		return math.Inf(1), false, nil
	}
	if strings.HasPrefix(s, "(") {
		v, err := strconv.ParseFloat(s[1:], 64)
		return v, true, err
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, false, err
}

// inScoreRange reports whether score falls within [min,max], honoring
// exclusivity at either end — shared by ZRANGEBYSCORE and ZCOUNT.
func inScoreRange(score, min float64, minExcl bool, max float64, maxExcl bool) bool {
	if minExcl {
		if score <= min {
			return false
		}
	} else if score < min {
		return false
	}
	if maxExcl {
		if score >= max {
			return false
		}
	} else if score > max {
		return false
	}
	return true
}

// lpushCommand implements LPUSH key value [value ...]. Each value is pushed
// one at a time, matching Redis's documented one-at-a-time semantics:
// LPUSH k a b c leaves the list as [c, b, a].
func lpushCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("lpush")
	}
	v, errReply := store.mutable(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	for _, val := range args[1:] {
		v.list = append([]string{val}, v.list...)
	}
	return integer(len(v.list))
}

// rpushCommand implements RPUSH key value [value ...]: RPUSH k a b c leaves
// the list as [a, b, c].
func rpushCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("rpush")
	}
	v, errReply := store.mutable(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	v.list = append(v.list, args[1:]...)
	return integer(len(v.list))
}

// listPopGeneric implements LPOP/RPOP key [count]. fromHead selects which
// end elements are removed from. With no count, pops one element and
// replies a bulk string (or nilBulk if the key's absent). With a count, pops
// up to count (clamped to the list's length) and replies an array — or
// nilArray if the key's absent, distinct from the empty array a count of 0
// (or more than available) on an existing key would give.
func listPopGeneric(store *Datastore, args []string, name string, fromHead bool) string {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs(name)
	}
	key := args[0]
	hasCount := len(args) == 2
	count := 1
	if hasCount {
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return errNotInteger
		}
		if n < 0 {
			return errMustBePositive
		}
		count = n
	}

	v, found, errReply := store.lookup(key, typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		if hasCount {
			return nilArray
		}
		return nilBulk
	}

	if count > len(v.list) {
		count = len(v.list)
	}

	var popped []string
	if fromHead {
		popped = append([]string(nil), v.list[:count]...)
		v.list = v.list[count:]
	} else {
		popped = append([]string(nil), v.list[len(v.list)-count:]...)
		for i, j := 0, len(popped)-1; i < j; i, j = i+1, j-1 {
			popped[i], popped[j] = popped[j], popped[i]
		}
		v.list = v.list[:len(v.list)-count]
	}
	if len(v.list) == 0 {
		store.remove(key)
	}

	if !hasCount {
		return bulkString(popped[0])
	}
	elems := make([]string, len(popped))
	for i, p := range popped {
		elems[i] = bulkString(p)
	}
	return arrayReply(elems...)
}

func lpopCommand(store *Datastore, args []string) string {
	return listPopGeneric(store, args, "lpop", true)
}

func rpopCommand(store *Datastore, args []string) string {
	return listPopGeneric(store, args, "rpop", false)
}

func lrangeCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("lrange")
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}

	start, stop = normalizeRange(start, stop, len(v.list))
	if start > stop {
		return emptyArray
	}

	elems := make([]string, 0, stop-start+1)
	for _, item := range v.list[start : stop+1] {
		elems = append(elems, bulkString(item))
	}
	return arrayReply(elems...)
}

func llenCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("llen")
	}
	v, found, errReply := store.lookup(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.list))
}

// lremCommand implements LREM key count value. count > 0 removes the first
// count occurrences head-to-tail; count < 0 removes the first |count|
// occurrences tail-to-head; count == 0 removes every occurrence.
func lremCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("lrem")
	}
	key, target := args[0], args[2]
	count, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(key, typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	removed := 0
	switch {
	case count >= 0:
		limit := count // 0 means unlimited, matched by never hitting the cap below
		result := make([]string, 0, len(v.list))
		for _, item := range v.list {
			if item == target && (limit == 0 || removed < limit) {
				removed++
				continue
			}
			result = append(result, item)
		}
		v.list = result
	default:
		limit := -count
		result := append([]string(nil), v.list...)
		for i := len(result) - 1; i >= 0 && removed < limit; i-- {
			if result[i] == target {
				result = append(result[:i], result[i+1:]...)
				removed++
			}
		}
		v.list = result
	}

	if len(v.list) == 0 {
		store.remove(key)
	}
	return integer(removed)
}

func lsetCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("lset")
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return "-ERR no such key\r\n"
	}

	if index < 0 {
		index += len(v.list)
	}
	if index < 0 || index >= len(v.list) {
		return "-ERR index out of range\r\n"
	}
	v.list[index] = args[2]
	return okReply
}

// linsertCommand implements LINSERT key BEFORE|AFTER pivot value, replying
// the new length, -1 if pivot isn't found, or 0 if key is absent.
func linsertCommand(store *Datastore, args []string) string {
	if len(args) != 4 {
		return wrongArgs("linsert")
	}
	key, where, pivot, val := args[0], strings.ToUpper(args[1]), args[2], args[3]
	if where != "BEFORE" && where != "AFTER" {
		return errSyntax
	}

	v, found, errReply := store.lookup(key, typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	idx := -1
	for i, item := range v.list {
		if item == pivot {
			idx = i
			break
		}
	}
	if idx == -1 {
		return integer(-1)
	}

	insertAt := idx
	if where == "AFTER" {
		insertAt++
	}
	v.list = append(v.list[:insertAt], append([]string{val}, v.list[insertAt:]...)...)
	return integer(len(v.list))
}

// ltrimCommand implements LTRIM key start stop: always replies +OK, even for
// an absent key, matching Redis.
func ltrimCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("ltrim")
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(args[0], typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return okReply
	}

	start, stop = normalizeRange(start, stop, len(v.list))
	if start > stop {
		store.remove(args[0])
		return okReply
	}
	v.list = append([]string(nil), v.list[start:stop+1]...)
	if len(v.list) == 0 {
		store.remove(args[0])
	}
	return okReply
}

// lmoveCommand implements LMOVE source destination LEFT|RIGHT LEFT|RIGHT.
// destination's kind is checked before source is popped, so a WRONGTYPE on
// destination never loses the popped element — safe because stateProcessor
// is the only goroutine ever touching store, so nothing can observe the
// half-done state in between. source == destination (rotate-in-place) works
// correctly for free: both resolve to the same *value.
func lmoveCommand(store *Datastore, args []string) string {
	if len(args) != 4 {
		return wrongArgs("lmove")
	}
	src, dst := args[0], args[1]
	fromWhere, toWhere := strings.ToUpper(args[2]), strings.ToUpper(args[3])
	if (fromWhere != "LEFT" && fromWhere != "RIGHT") || (toWhere != "LEFT" && toWhere != "RIGHT") {
		return errSyntax
	}

	srcVal, found, errReply := store.lookup(src, typeList)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	if _, _, errReply := store.lookup(dst, typeList); errReply != "" {
		return errReply
	}

	var moved string
	if fromWhere == "LEFT" {
		moved = srcVal.list[0]
		srcVal.list = srcVal.list[1:]
	} else {
		moved = srcVal.list[len(srcVal.list)-1]
		srcVal.list = srcVal.list[:len(srcVal.list)-1]
	}
	if len(srcVal.list) == 0 {
		store.remove(src)
	}

	dstVal, errReply := store.mutable(dst, typeList)
	if errReply != "" {
		return errReply
	}
	if toWhere == "LEFT" {
		dstVal.list = append([]string{moved}, dstVal.list...)
	} else {
		dstVal.list = append(dstVal.list, moved)
	}
	return bulkString(moved)
}

// formatScore renders score the way Redis does: shortest exact round-trip,
// so a whole-number score like 1.0 renders "1", not "1.0".
func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

// cloneValue deep-copies v's populated payload (and its TTL, matching Redis:
// COPY carries the TTL to the new name) so mutating the copy can never reach
// back into the original — unlike RENAME's move, COPY must leave the source
// completely independent.
func cloneValue(v *value) *value {
	clone := &value{kind: v.kind, str: v.str, expireAt: v.expireAt}
	if v.hash != nil {
		clone.hash = make(map[string]string, len(v.hash))
		for f, val := range v.hash {
			clone.hash[f] = val
		}
	}
	if v.set != nil {
		clone.set = make(map[string]struct{}, len(v.set))
		for m := range v.set {
			clone.set[m] = struct{}{}
		}
	}
	if v.list != nil {
		clone.list = append([]string(nil), v.list...)
	}
	if v.zset != nil {
		clone.zset = make(map[string]float64, len(v.zset))
		for m, s := range v.zset {
			clone.zset[m] = s
		}
	}
	return clone
}

// incrByGeneric implements INCR/DECR/INCRBY/DECRBY: parses the string at key
// as an int64 (0 if absent, errNotInteger if unparseable), adds delta with
// an explicit overflow check, and replies the new value via integer64.
func incrByGeneric(store *Datastore, key string, delta int64) string {
	v, errReply := store.mutable(key, typeString)
	if errReply != "" {
		return errReply
	}
	var current int64
	if v.str != "" {
		n, err := strconv.ParseInt(v.str, 10, 64)
		if err != nil {
			return errNotInteger
		}
		current = n
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return errOverflow
	}
	current += delta
	v.str = strconv.FormatInt(current, 10)
	return integer64(current)
}

// getsetCommand implements GETSET key value: returns the old value (nil if
// absent) and always overwrites, clearing any TTL like plain SET does.
func getsetCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("getset")
	}
	v, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	old := nilBulk
	if found {
		old = bulkString(v.str)
	}
	store.replace(args[0], &value{kind: typeString, str: args[1]})
	return old
}

// getdelCommand implements GETDEL key: GET plus a DEL in one op.
func getdelCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("getdel")
	}
	v, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	result := bulkString(v.str)
	store.remove(args[0])
	return result
}

func appendCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("append")
	}
	v, errReply := store.mutable(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	v.str += args[1]
	return integer(len(v.str))
}

func strlenCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("strlen")
	}
	v, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.str))
}

// setnxCommand implements SETNX key value: succeeds only if key doesn't
// exist at all, regardless of what type currently occupies it (matches
// Redis: NX here is keyspace-wide, not string-specific).
func setnxCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("setnx")
	}
	if _, found := store.get(args[0]); found {
		return integer(0)
	}
	store.replace(args[0], &value{kind: typeString, str: args[1]})
	return integer(1)
}

// setrangeCommand implements SETRANGE key offset value: zero-pads with \x00
// up to offset if the string is shorter, matching Redis. An empty value
// against a missing key is a no-op returning 0 rather than creating an
// empty string (Redis's own documented edge case).
func setrangeCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("setrange")
	}
	offset, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	if offset < 0 {
		return "-ERR offset is out of range\r\n"
	}
	val := args[2]

	existing, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	if !found && val == "" {
		return integer(0)
	}

	var current string
	if found {
		current = existing.str
	}
	buf := []byte(current)
	if offset+len(val) > len(buf) {
		grown := make([]byte, offset+len(val))
		copy(grown, buf)
		buf = grown
	}
	copy(buf[offset:], val)

	v, errReply := store.mutable(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	v.str = string(buf)
	return integer(len(v.str))
}

// getrangeCommand implements GETRANGE key start end, reusing the same
// inclusive/negative-index rules normalizeRange already applies to
// lists/sorted sets. A missing key is an empty string, not nil (Redis).
func getrangeCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("getrange")
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	if !found {
		return bulkString("")
	}

	start, end = normalizeRange(start, end, len(v.str))
	if start > end {
		return bulkString("")
	}
	return bulkString(v.str[start : end+1])
}

func incrCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("incr")
	}
	return incrByGeneric(store, args[0], 1)
}

func decrCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("decr")
	}
	return incrByGeneric(store, args[0], -1)
}

func incrbyCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("incrby")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInteger
	}
	return incrByGeneric(store, args[0], delta)
}

func decrbyCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("decrby")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInteger
	}
	if delta == math.MinInt64 {
		return errOverflow // negating MinInt64 would itself overflow
	}
	return incrByGeneric(store, args[0], -delta)
}

func incrbyfloatCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("incrbyfloat")
	}
	delta, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return errNotFloat
	}
	v, errReply := store.mutable(args[0], typeString)
	if errReply != "" {
		return errReply
	}
	var current float64
	if v.str != "" {
		n, err := strconv.ParseFloat(v.str, 64)
		if err != nil {
			return errNotFloat
		}
		current = n
	}
	current += delta
	v.str = formatScore(current)
	return bulkString(v.str)
}

// msetCommand implements MSET key value [key value ...]: unconditional and
// trivially atomic — one command execution on the single stateProcessor
// goroutine.
func msetCommand(store *Datastore, args []string) string {
	if len(args) == 0 || len(args)%2 != 0 {
		return wrongArgs("mset")
	}
	for i := 0; i < len(args); i += 2 {
		store.replace(args[i], &value{kind: typeString, str: args[i+1]})
	}
	return okReply
}

// msetnxCommand implements MSETNX: true all-or-nothing — every key is
// checked via store.get before any of them are written, so one existing key
// blocks the entire write rather than leaving a partial one.
func msetnxCommand(store *Datastore, args []string) string {
	if len(args) == 0 || len(args)%2 != 0 {
		return wrongArgs("msetnx")
	}
	for i := 0; i < len(args); i += 2 {
		if _, found := store.get(args[i]); found {
			return integer(0)
		}
	}
	for i := 0; i < len(args); i += 2 {
		store.replace(args[i], &value{kind: typeString, str: args[i+1]})
	}
	return integer(1)
}

// mgetCommand implements MGET key [key ...]. The one WRONGTYPE exception in
// this codebase: Redis defines MGET as treating a non-string key as absent
// (nil for that position) rather than erroring the whole command.
func mgetCommand(store *Datastore, args []string) string {
	if len(args) == 0 {
		return wrongArgs("mget")
	}
	elems := make([]string, len(args))
	for i, key := range args {
		v, found := store.get(key)
		if found && v.kind == typeString {
			elems[i] = bulkString(v.str)
		} else {
			elems[i] = nilBulk
		}
	}
	return arrayReply(elems...)
}

// zaddCommand implements ZADD key [NX|XX] [CH] score member [score member
// ...]. NX/XX/CH only — not GT/LT (each would need a per-member condition
// against that member's own current score, not one shared comparison) or
// INCR (changes the reply from a count to a score-or-nil). XX on a key that
// doesn't exist yet must not create it: checked via a non-creating get
// before ever calling mutable, otherwise every member would be skipped into
// an empty zset that lingers as a phantom key.
func zaddCommand(store *Datastore, args []string) string {
	if len(args) < 3 {
		return wrongArgs("zadd")
	}
	key := args[0]

	var nx, xx, ch bool
	i := 1
loop:
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "NX":
			if xx {
				return errSyntax
			}
			nx = true
			i++
		case "XX":
			if nx {
				return errSyntax
			}
			xx = true
			i++
		case "CH":
			ch = true
			i++
		default:
			break loop
		}
	}

	pairs := args[i:]
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return wrongArgs("zadd")
	}

	existing, found := store.get(key)
	if found && existing.kind != typeZSet {
		return errWrongType
	}
	if xx && !found {
		return integer(0)
	}

	v, errReply := store.mutable(key, typeZSet)
	if errReply != "" {
		return errReply
	}

	added, changed := 0, 0
	for j := 0; j < len(pairs); j += 2 {
		score, err := strconv.ParseFloat(pairs[j], 64)
		if err != nil {
			return errNotFloat
		}
		member := pairs[j+1]
		old, exists := v.zset[member]
		if nx && exists {
			continue
		}
		if xx && !exists {
			continue
		}
		if !exists {
			added++
			changed++
			v.zset[member] = score
		} else if old != score {
			changed++
			v.zset[member] = score
		}
	}
	if ch {
		return integer(changed)
	}
	return integer(added)
}

func zscoreCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("zscore")
	}
	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	score, ok := v.zset[args[1]]
	if !ok {
		return nilBulk
	}
	return bulkString(formatScore(score))
}

func zcardCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("zcard")
	}
	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.zset))
}

func zrankCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("zrank")
	}
	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return nilBulk
	}
	if _, ok := v.zset[args[1]]; !ok {
		return nilBulk
	}
	for i, m := range sortedZMembers(v.zset) {
		if m.member == args[1] {
			return integer(i)
		}
	}
	return nilBulk // unreachable given the membership check above
}

// zincrbyCommand implements ZINCRBY key increment member, always via
// mutable: the target member is always present afterward, so there's no
// ZADD-XX-style phantom-empty-key risk. Go's zero-value float64 on a fresh
// map entry gives an absent member's "starts at 0" for free.
func zincrbyCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("zincrby")
	}
	incr, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return errNotFloat
	}
	v, errReply := store.mutable(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	newScore := v.zset[args[2]] + incr
	v.zset[args[2]] = newScore
	return bulkString(formatScore(newScore))
}

// zremCommand mirrors sremCommand's shape exactly: remove listed members,
// empty result removes the key.
func zremCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("zrem")
	}
	key, members := args[0], args[1:]

	v, found, errReply := store.lookup(key, typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	removed := 0
	for _, member := range members {
		if _, exists := v.zset[member]; exists {
			delete(v.zset, member)
			removed++
		}
	}
	if len(v.zset) == 0 {
		store.remove(key)
	}
	return integer(removed)
}

// zrangeCommand implements ZRANGE key start stop [WITHSCORES] — the classic
// index-based form only, not Redis 6.2's unified BYSCORE/BYLEX/REV/LIMIT
// syntax (a distinct, larger surface the plan's separate ZRANGE/
// ZRANGEBYSCORE listing doesn't imply).
func zrangeCommand(store *Datastore, args []string) string {
	if len(args) != 3 && len(args) != 4 {
		return wrongArgs("zrange")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(args[3]) != "WITHSCORES" {
			return errSyntax
		}
		withScores = true
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return errNotInteger
	}

	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}

	members := sortedZMembers(v.zset)
	start, stop = normalizeRange(start, stop, len(members))
	if start > stop {
		return emptyArray
	}

	elems := make([]string, 0, stop-start+1)
	for _, m := range members[start : stop+1] {
		elems = append(elems, bulkString(m.member))
		if withScores {
			elems = append(elems, bulkString(formatScore(m.score)))
		}
	}
	return arrayReply(elems...)
}

// zrangebyscoreCommand implements ZRANGEBYSCORE key min max [WITHSCORES]
// [LIMIT offset count].
func zrangebyscoreCommand(store *Datastore, args []string) string {
	if len(args) < 3 {
		return wrongArgs("zrangebyscore")
	}
	minScore, minExcl, err := parseScoreRange(args[1])
	if err != nil {
		return errMinMaxNotFloat
	}
	maxScore, maxExcl, err := parseScoreRange(args[2])
	if err != nil {
		return errMinMaxNotFloat
	}

	withScores := false
	offset, count := 0, -1
	i := 3
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return errSyntax
			}
			offset, err = strconv.Atoi(args[i+1])
			if err != nil {
				return errNotInteger
			}
			count, err = strconv.Atoi(args[i+2])
			if err != nil {
				return errNotInteger
			}
			i += 3
		default:
			return errSyntax
		}
	}

	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}

	var matches []zmember
	for _, m := range sortedZMembers(v.zset) {
		if inScoreRange(m.score, minScore, minExcl, maxScore, maxExcl) {
			matches = append(matches, m)
		}
	}
	if offset > 0 {
		if offset >= len(matches) {
			matches = nil
		} else {
			matches = matches[offset:]
		}
	}
	if count >= 0 && count < len(matches) {
		matches = matches[:count]
	}

	elems := make([]string, 0, len(matches)*2)
	for _, m := range matches {
		elems = append(elems, bulkString(m.member))
		if withScores {
			elems = append(elems, bulkString(formatScore(m.score)))
		}
	}
	return arrayReply(elems...)
}

func zcountCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("zcount")
	}
	minScore, minExcl, err := parseScoreRange(args[1])
	if err != nil {
		return errMinMaxNotFloat
	}
	maxScore, maxExcl, err := parseScoreRange(args[2])
	if err != nil {
		return errMinMaxNotFloat
	}

	v, found, errReply := store.lookup(args[0], typeZSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}

	count := 0
	for _, score := range v.zset {
		if inScoreRange(score, minScore, minExcl, maxScore, maxExcl) {
			count++
		}
	}
	return integer(count)
}

func hexistsCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("hexists")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	if _, ok := v.hash[args[1]]; ok {
		return integer(1)
	}
	return integer(0)
}

func hlenCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("hlen")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.hash))
}

func hkeysCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("hkeys")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}
	elems := make([]string, 0, len(v.hash))
	for field := range v.hash {
		elems = append(elems, bulkString(field))
	}
	return arrayReply(elems...)
}

func hvalsCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("hvals")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}
	elems := make([]string, 0, len(v.hash))
	for _, val := range v.hash {
		elems = append(elems, bulkString(val))
	}
	return arrayReply(elems...)
}

// hgetallCommand implements HGETALL key: a flat array of alternating
// field/value bulk strings — the same shape CONFIG GET already produces.
func hgetallCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("hgetall")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}
	elems := make([]string, 0, len(v.hash)*2)
	for field, val := range v.hash {
		elems = append(elems, bulkString(field), bulkString(val))
	}
	return arrayReply(elems...)
}

// hmsetCommand implements HMSET key field value [field value ...]: the same
// mutation loop as hsetCommand, replying +OK instead of a count (Redis's
// deprecated-but-still-wired alias for multi-field HSET).
func hmsetCommand(store *Datastore, args []string) string {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs("hmset")
	}
	v, errReply := store.mutable(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	for i := 1; i < len(args); i += 2 {
		v.hash[args[i]] = args[i+1]
	}
	return okReply
}

// hmgetCommand implements HMGET key field [field ...]. Unlike MGET, a
// WRONGTYPE key errors the whole command — the two commands are not
// symmetric here (verified against real Redis behavior).
func hmgetCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("hmget")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	elems := make([]string, len(args)-1)
	for i, field := range args[1:] {
		if !found {
			elems[i] = nilBulk
			continue
		}
		if val, ok := v.hash[field]; ok {
			elems[i] = bulkString(val)
		} else {
			elems[i] = nilBulk
		}
	}
	return arrayReply(elems...)
}

func hsetnxCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("hsetnx")
	}
	v, errReply := store.mutable(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if _, exists := v.hash[args[1]]; exists {
		return integer(0)
	}
	v.hash[args[1]] = args[2]
	return integer(1)
}

// hincrbyCommand implements HINCRBY key field increment: same overflow-
// checked pattern as INCRBY, reading/writing v.hash[field] instead of v.str.
func hincrbyCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("hincrby")
	}
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return errNotInteger
	}
	v, errReply := store.mutable(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	var current int64
	if existing, ok := v.hash[args[1]]; ok {
		n, err := strconv.ParseInt(existing, 10, 64)
		if err != nil {
			return errNotInteger
		}
		current = n
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return errOverflow
	}
	current += delta
	v.hash[args[1]] = strconv.FormatInt(current, 10)
	return integer64(current)
}

func hincrbyfloatCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("hincrbyfloat")
	}
	delta, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return errNotFloat
	}
	v, errReply := store.mutable(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	var current float64
	if existing, ok := v.hash[args[1]]; ok {
		n, err := strconv.ParseFloat(existing, 64)
		if err != nil {
			return errNotFloat
		}
		current = n
	}
	current += delta
	result := formatScore(current)
	v.hash[args[1]] = result
	return bulkString(result)
}

func hstrlenCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("hstrlen")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.hash[args[1]]))
}

// randomFields returns up to n distinct keys from fields — the "distinct,
// positive count" half of HRANDFIELD/SRANDMEMBER. Go's randomized map
// iteration order is the randomness source, same reasoning already relied
// on for RANDOMKEY.
func randomDistinct(fields []string, n int) []string {
	if n > len(fields) {
		n = len(fields)
	}
	return append([]string(nil), fields[:n]...)
}

// randomWithRepeats returns exactly n picks from fields, allowing repeats —
// the "negative count" half of HRANDFIELD/SRANDMEMBER. Uses math/rand's
// global source: no seed-injection seam, tests check structural invariants
// rather than exact output (same call already made for RANDOMKEY).
func randomWithRepeats(fields []string, n int) []string {
	if len(fields) == 0 {
		return nil
	}
	picks := make([]string, n)
	for i := range picks {
		picks[i] = fields[mathrand.Intn(len(fields))]
	}
	return picks
}

// hrandfieldCommand implements HRANDFIELD key [count [WITHVALUES]].
func hrandfieldCommand(store *Datastore, args []string) string {
	if len(args) < 1 || len(args) > 3 {
		return wrongArgs("hrandfield")
	}
	v, found, errReply := store.lookup(args[0], typeHash)
	if errReply != "" {
		return errReply
	}

	if len(args) == 1 {
		if !found {
			return nilBulk
		}
		for field := range v.hash {
			return bulkString(field)
		}
		return nilBulk // unreachable: a hash key is never empty
	}

	count, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	withValues := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) != "WITHVALUES" {
			return errSyntax
		}
		withValues = true
	}
	if !found {
		return emptyArray
	}

	fields := make([]string, 0, len(v.hash))
	for field := range v.hash {
		fields = append(fields, field)
	}
	var picks []string
	if count >= 0 {
		picks = randomDistinct(fields, count)
	} else {
		picks = randomWithRepeats(fields, -count)
	}

	elems := make([]string, 0, len(picks)*2)
	for _, f := range picks {
		elems = append(elems, bulkString(f))
		if withValues {
			elems = append(elems, bulkString(v.hash[f]))
		}
	}
	return arrayReply(elems...)
}

func smembersCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("smembers")
	}
	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return emptyArray
	}
	elems := make([]string, 0, len(v.set))
	for m := range v.set {
		elems = append(elems, bulkString(m))
	}
	return arrayReply(elems...)
}

func sismemberCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("sismember")
	}
	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	if _, ok := v.set[args[1]]; ok {
		return integer(1)
	}
	return integer(0)
}

func smismemberCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("smismember")
	}
	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}
	elems := make([]string, len(args)-1)
	for i, m := range args[1:] {
		if found {
			if _, ok := v.set[m]; ok {
				elems[i] = integer(1)
				continue
			}
		}
		elems[i] = integer(0)
	}
	return arrayReply(elems...)
}

func scardCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("scard")
	}
	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	return integer(len(v.set))
}

// spopCommand implements SPOP key [count]. Contrast with LPOP: SPOP with a
// count on a missing key replies emptyArray, not nilArray — a different
// documented convention, not shared with listPopGeneric.
func spopCommand(store *Datastore, args []string) string {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("spop")
	}
	hasCount := len(args) == 2
	count := 1
	if hasCount {
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return errNotInteger
		}
		if n < 0 {
			return errMustBePositive
		}
		count = n
	}

	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		if hasCount {
			return emptyArray
		}
		return nilBulk
	}

	members := make([]string, 0, len(v.set))
	for m := range v.set {
		members = append(members, m)
	}
	if count > len(members) {
		count = len(members)
	}
	popped := members[:count]
	for _, m := range popped {
		delete(v.set, m)
	}
	if len(v.set) == 0 {
		store.remove(args[0])
	}

	if !hasCount {
		return bulkString(popped[0])
	}
	elems := make([]string, len(popped))
	for i, m := range popped {
		elems[i] = bulkString(m)
	}
	return arrayReply(elems...)
}

// srandmemberCommand implements SRANDMEMBER key [count]: same positive/
// negative-count semantics as HRANDFIELD, no WITHVALUES (sets have none),
// and read-only (unlike SPOP, nothing is removed).
func srandmemberCommand(store *Datastore, args []string) string {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("srandmember")
	}
	v, found, errReply := store.lookup(args[0], typeSet)
	if errReply != "" {
		return errReply
	}

	if len(args) == 1 {
		if !found {
			return nilBulk
		}
		for m := range v.set {
			return bulkString(m)
		}
		return nilBulk // unreachable: a set key is never empty
	}

	count, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInteger
	}
	if !found {
		return emptyArray
	}

	members := make([]string, 0, len(v.set))
	for m := range v.set {
		members = append(members, m)
	}
	var picks []string
	if count >= 0 {
		picks = randomDistinct(members, count)
	} else {
		picks = randomWithRepeats(members, -count)
	}
	elems := make([]string, len(picks))
	for i, m := range picks {
		elems[i] = bulkString(m)
	}
	return arrayReply(elems...)
}

// smoveCommand implements SMOVE source destination member: the same
// "check destination's kind before mutating source" pattern LMOVE already
// established, so a WRONGTYPE on destination never loses member from source.
func smoveCommand(store *Datastore, args []string) string {
	if len(args) != 3 {
		return wrongArgs("smove")
	}
	src, dst, member := args[0], args[1], args[2]

	srcVal, found, errReply := store.lookup(src, typeSet)
	if errReply != "" {
		return errReply
	}
	if !found {
		return integer(0)
	}
	if _, ok := srcVal.set[member]; !ok {
		return integer(0)
	}
	if _, _, errReply := store.lookup(dst, typeSet); errReply != "" {
		return errReply
	}

	delete(srcVal.set, member)
	if len(srcVal.set) == 0 {
		store.remove(src)
	}

	dstVal, errReply := store.mutable(dst, typeSet)
	if errReply != "" {
		return errReply
	}
	dstVal.set[member] = struct{}{}
	return integer(1)
}

// collectSets fetches each key's member set, treating an absent key as an
// empty set (Redis's own semantics for the whole SUNION/SINTER/SDIFF
// family) and erroring WRONGTYPE if any existing key isn't a set.
func collectSets(store *Datastore, keys []string) (sets []map[string]struct{}, errReply string) {
	sets = make([]map[string]struct{}, len(keys))
	for i, k := range keys {
		v, found, errReply := store.lookup(k, typeSet)
		if errReply != "" {
			return nil, errReply
		}
		if found {
			sets[i] = v.set
		}
	}
	return sets, ""
}

func unionSets(sets []map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	for _, s := range sets {
		for m := range s {
			result[m] = struct{}{}
		}
	}
	return result
}

func intersectSets(sets []map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	if len(sets) == 0 {
		return result
	}
	for m := range sets[0] {
		inAll := true
		for _, s := range sets[1:] {
			if _, ok := s[m]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			result[m] = struct{}{}
		}
	}
	return result
}

func diffSets(sets []map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	if len(sets) == 0 {
		return result
	}
	for m := range sets[0] {
		excluded := false
		for _, s := range sets[1:] {
			if _, ok := s[m]; ok {
				excluded = true
				break
			}
		}
		if !excluded {
			result[m] = struct{}{}
		}
	}
	return result
}

func setResultReply(members map[string]struct{}) string {
	elems := make([]string, 0, len(members))
	for m := range members {
		elems = append(elems, bulkString(m))
	}
	return arrayReply(elems...)
}

func sunionCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("sunion")
	}
	sets, errReply := collectSets(store, args)
	if errReply != "" {
		return errReply
	}
	return setResultReply(unionSets(sets))
}

func sinterCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("sinter")
	}
	sets, errReply := collectSets(store, args)
	if errReply != "" {
		return errReply
	}
	return setResultReply(intersectSets(sets))
}

func sdiffCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("sdiff")
	}
	sets, errReply := collectSets(store, args)
	if errReply != "" {
		return errReply
	}
	return setResultReply(diffSets(sets))
}

// storeSetResult implements the *STORE variants: writes result to
// destination (removing it if the result is empty), replying the
// cardinality — matches Redis.
func storeSetResult(store *Datastore, destination string, result map[string]struct{}) string {
	if len(result) == 0 {
		store.remove(destination)
		return integer(0)
	}
	store.replace(destination, &value{kind: typeSet, set: result})
	return integer(len(result))
}

func sunionstoreCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("sunionstore")
	}
	sets, errReply := collectSets(store, args[1:])
	if errReply != "" {
		return errReply
	}
	return storeSetResult(store, args[0], unionSets(sets))
}

func sinterstoreCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("sinterstore")
	}
	sets, errReply := collectSets(store, args[1:])
	if errReply != "" {
		return errReply
	}
	return storeSetResult(store, args[0], intersectSets(sets))
}

func sdiffstoreCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("sdiffstore")
	}
	sets, errReply := collectSets(store, args[1:])
	if errReply != "" {
		return errReply
	}
	return storeSetResult(store, args[0], diffSets(sets))
}

// sintercardCommand implements SINTERCARD numkeys key [key...] [LIMIT
// limit]: the intersection's cardinality without storing it. LIMIT caps the
// reported count rather than early-exiting the computation — correct
// result, not Redis's own early-exit optimization.
func sintercardCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return wrongArgs("sintercard")
	}
	numkeys, err := strconv.Atoi(args[0])
	if err != nil || numkeys < 1 {
		return errNotInteger
	}
	if len(args) < 1+numkeys {
		return wrongArgs("sintercard")
	}
	keys := args[1 : 1+numkeys]
	rest := args[1+numkeys:]

	limit := -1
	if len(rest) > 0 {
		if len(rest) != 2 || strings.ToUpper(rest[0]) != "LIMIT" {
			return errSyntax
		}
		n, err := strconv.Atoi(rest[1])
		if err != nil || n < 0 {
			return errNotInteger
		}
		if n > 0 {
			limit = n
		}
	}

	sets, errReply := collectSets(store, keys)
	if errReply != "" {
		return errReply
	}
	count := len(intersectSets(sets))
	if limit >= 0 && count > limit {
		count = limit
	}
	return integer(count)
}

// existsCommand implements EXISTS key [key ...], counting duplicates:
// EXISTS k k on an existing k replies 2 (matches Redis).
func existsCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("exists")
	}
	count := 0
	for _, key := range args {
		if _, found := store.get(key); found {
			count++
		}
	}
	return integer(count)
}

// unlinkCommand is identical to delCommand: Redis's UNLINK/DEL distinction
// (asynchronous memory reclaim) doesn't exist in a model with no separate
// reclaim path. A separate registry entry rather than an alias, since a
// future divergence shouldn't require un-aliasing.
func unlinkCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("unlink")
	}
	removed := 0
	for _, key := range args {
		if _, found := store.get(key); found {
			store.remove(key)
			removed++
		}
	}
	return integer(removed)
}

// keysCommand implements KEYS pattern: a full scan, not SCAN — enumerating
// everything in one shot is exactly what a Go map already supports; the
// deferred problem was specifically a stable cursor across multiple calls,
// which KEYS doesn't need.
func keysCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("keys")
	}
	elems := make([]string, 0, len(store.keys))
	for key := range store.keys {
		if matched, err := path.Match(args[0], key); err == nil && matched {
			elems = append(elems, bulkString(key))
		}
	}
	return arrayReply(elems...)
}

// renameCommand implements RENAME key newkey: moves the same *value object
// (so its TTL travels for free — expireAt lives on the value, not looked up
// separately), replying an error if key doesn't exist. key == newkey is a
// no-op success; without that guard, replace-then-remove would delete the
// only copy.
func renameCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("rename")
	}
	if args[0] == args[1] {
		if _, found := store.get(args[0]); !found {
			return errNoSuchKey
		}
		return okReply
	}
	v, found := store.get(args[0])
	if !found {
		return errNoSuchKey
	}
	store.replace(args[1], v)
	store.remove(args[0])
	return okReply
}

// renamenxCommand implements RENAMENX key newkey: like RENAME, but refuses
// if newkey already exists.
func renamenxCommand(store *Datastore, args []string) string {
	if len(args) != 2 {
		return wrongArgs("renamenx")
	}
	v, found := store.get(args[0])
	if !found {
		return errNoSuchKey
	}
	if args[0] == args[1] {
		return integer(0)
	}
	if _, found := store.get(args[1]); found {
		return integer(0)
	}
	store.replace(args[1], v)
	store.remove(args[0])
	return integer(1)
}

// randomkeyCommand iterates store.keys (map order is already Go's own
// randomization), lazily expiring and skipping anything found due so a
// not-yet-swept expired key is never returned.
func randomkeyCommand(store *Datastore, args []string) string {
	if len(args) != 0 {
		return wrongArgs("randomkey")
	}
	for key := range store.keys {
		if _, found := store.get(key); found {
			return bulkString(key)
		}
	}
	return nilBulk
}

// dbsizeCommand replies len(store.keys) directly — intentionally not
// expiry-aware, matching real Redis's own DBSIZE (a raw count; lazy/active
// expiration catches up independently).
func dbsizeCommand(store *Datastore, args []string) string {
	if len(args) != 0 {
		return wrongArgs("dbsize")
	}
	return integer(len(store.keys))
}

// flushCommand implements FLUSHDB/FLUSHALL: identical, since there's one
// logical DB. Removes every key (rather than replacing the map wholesale)
// so every previously-live prefixed name — and any expiry entry — gets
// correctly staled for the next save. An optional trailing ASYNC/SYNC is
// accepted and ignored (always effectively synchronous here).
func flushCommand(store *Datastore, name string, args []string) string {
	if len(args) > 1 {
		return wrongArgs(name)
	}
	if len(args) == 1 {
		mode := strings.ToUpper(args[0])
		if mode != "ASYNC" && mode != "SYNC" {
			return errSyntax
		}
	}
	for key := range store.keys {
		store.remove(key)
	}
	return okReply
}

func flushdbCommand(store *Datastore, args []string) string {
	return flushCommand(store, "flushdb", args)
}

func flushallCommand(store *Datastore, args []string) string {
	return flushCommand(store, "flushall", args)
}

// copyCommand implements COPY source destination [REPLACE]: a genuine deep
// copy via cloneValue (unlike RENAME's move), so mutating one copy never
// reaches the other. Fails (0) if destination exists and REPLACE isn't given.
func copyCommand(store *Datastore, args []string) string {
	if len(args) < 2 || len(args) > 3 {
		return wrongArgs("copy")
	}
	replace := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) != "REPLACE" {
			return errSyntax
		}
		replace = true
	}
	if args[0] == args[1] {
		return integer(0)
	}

	v, found := store.get(args[0])
	if !found {
		return integer(0)
	}
	if _, destFound := store.get(args[1]); destFound && !replace {
		return integer(0)
	}
	store.replace(args[1], cloneValue(v))
	return integer(1)
}

func echoCommand(store *Datastore, args []string) string {
	if len(args) != 1 {
		return wrongArgs("echo")
	}
	return bulkString(args[0])
}

// timeCommand implements TIME: unix seconds and the microsecond remainder,
// read from store.now() rather than time.Now() directly (consistent with
// every other time-aware command).
func timeCommand(store *Datastore, args []string) string {
	if len(args) != 0 {
		return wrongArgs("time")
	}
	now := store.now()
	return arrayReply(
		bulkString(strconv.FormatInt(now.Unix(), 10)),
		bulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)),
	)
}

// lastsaveCommand reads store.lastSaveUnix back, set by stateProcessor after
// every successful SAVE/BGSAVE/periodic-tick save.
func lastsaveCommand(store *Datastore, args []string) string {
	if len(args) != 0 {
		return wrongArgs("lastsave")
	}
	return integer64(store.lastSaveUnix)
}

// infoCommand implements INFO [section]: a single bulk string in Redis's own
// key:value\r\n line format. section is accepted but ignored — always the
// same reduced field set, not Redis's ~15 real sections (same "reduced but
// wire-valid" call already made for CLIENT LIST and COMMAND DOCS).
func infoCommand(store *Datastore, args []string) string {
	if len(args) > 1 {
		return wrongArgs("info")
	}
	uptime := int64(store.now().Sub(store.startedAt).Seconds())
	lines := []string{
		"# Server",
		"cachekv_version:0.1.0",
		fmt.Sprintf("uptime_in_seconds:%d", uptime),
		"# Keyspace",
		fmt.Sprintf("db0:keys=%d", len(store.keys)),
		"# Persistence",
		fmt.Sprintf("rdb_last_save_time:%d", store.lastSaveUnix),
	}
	return bulkString(strings.Join(lines, "\r\n") + "\r\n")
}

// debugCommand implements only DEBUG SLEEP seconds, as a literal time.Sleep
// — genuinely blocking the single stateProcessor goroutine for every
// client, matching real Redis's own DEBUG SLEEP (documented as blocking its
// single main thread too; test suites reach for it specifically for that).
// Every other subcommand errors rather than silently no-op'ing.
func debugCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return wrongArgs("debug")
	}
	if strings.ToUpper(args[0]) == "SLEEP" {
		if len(args) != 2 {
			return wrongArgs("debug")
		}
		seconds, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return errNotFloat
		}
		time.Sleep(time.Duration(seconds * float64(time.Second)))
		return okReply
	}
	return fmt.Sprintf("-ERR unknown DEBUG subcommand '%s'\r\n", args[0])
}
