package main

import "fmt"

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
						cmd.Resp <- "+OK\r\n"
					}
				}
			} else if cmd.Resp != nil {
				cmd.Resp <- "+OK\r\n"
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

type Datastore struct {
	Strings map[string]string
	Hashes  map[string]map[string]string
	Sets    map[string]map[string]struct{}
}

func NewDatastore() *Datastore {
	return &Datastore{
		Strings: make(map[string]string),
		Hashes:  make(map[string]map[string]string),
		Sets:    make(map[string]map[string]struct{}),
	}
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
}

func pingCommand(store *Datastore, args []string) string {
	if len(args) > 0 {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
	}
	return "+PONG\r\n"
}

func setCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'set' command\r\n"
	}
	store.Strings[args[0]] = args[1]
	return "+OK\r\n"
}

func getCommand(store *Datastore, args []string) string {
	if len(args) < 1 {
		return "-ERR wrong number of arguments for 'get' command\r\n"
	}
	val, exists := store.Strings[args[0]]
	if !exists {
		return "$-1\r\n" // Redis standard for (nil)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func hsetCommand(store *Datastore, args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'hset' command\r\n"
	}
	key, field, val := args[0], args[1], args[2]

	// Initialize the inner map if it doesn't exist
	if store.Hashes[key] == nil {
		store.Hashes[key] = make(map[string]string)
	}

	store.Hashes[key][field] = val
	return ":1\r\n" // Integer reply indicating 1 field was added
}

func hgetCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'hget' command\r\n"
	}
	key, field := args[0], args[1]

	fields, exists := store.Hashes[key]
	if !exists {
		return "$-1\r\n" // Redis standard for (nil)
	}
	val, exists := fields[field]
	if !exists {
		return "$-1\r\n" // Redis standard for (nil)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func saddCommand(store *Datastore, args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'sadd' command\r\n"
	}
	key, members := args[0], args[1:]

	if store.Sets[key] == nil {
		store.Sets[key] = make(map[string]struct{})
	}

	added := 0
	for _, member := range members {
		if _, exists := store.Sets[key][member]; !exists {
			store.Sets[key][member] = struct{}{}
			added++
		}
	}
	return fmt.Sprintf(":%d\r\n", added) // Integer reply of members added
}
