package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// clientEntry describes one currently-open cache-store connection, backing
// CLIENT ID/GETNAME/SETNAME/LIST.
type clientEntry struct {
	id          int64
	addr        string
	localAddr   string
	name        string
	connectedAt time.Time
}

// clientRegistry tracks every open connection. Unlike Datastore — centralised
// onto the single stateProcessor goroutine by design, so it needs no locking
// — connections are inherently one-goroutine-per-client: they come and go
// concurrently from many handleConnection goroutines, so listing "every
// connection right now" genuinely needs its own lock. This registry never
// touches Datastore and is never gated by stateProcessor.
type clientRegistry struct {
	mu      sync.Mutex
	clients map[int64]*clientEntry
}

var nextClientID atomic.Int64

func newClientRegistry() *clientRegistry {
	return &clientRegistry{clients: make(map[int64]*clientEntry)}
}

// register adds a new connection and returns its entry, with a freshly
// assigned, process-unique id.
func (r *clientRegistry) register(addr, localAddr string) *clientEntry {
	entry := &clientEntry{
		id:          nextClientID.Add(1),
		addr:        addr,
		localAddr:   localAddr,
		connectedAt: time.Now(),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[entry.id] = entry
	return entry
}

// unregister removes a connection when it closes.
func (r *clientRegistry) unregister(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, id)
}

// setName updates the display name recorded for id, e.g. for CLIENT LIST.
func (r *clientRegistry) setName(id int64, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.clients[id]; ok {
		entry.name = name
	}
}

// list renders the CLIENT LIST body: one line per connection, oldest first.
// A reduced field set of real Redis's ~25-field CLIENT LIST — most clients
// that parse this output only look at a handful (id, addr, name).
func (r *clientRegistry) list() string {
	r.mu.Lock()
	entries := make([]*clientEntry, 0, len(r.clients))
	for _, entry := range r.clients {
		entries = append(entries, entry)
	}
	r.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	lines := make([]string, len(entries))
	now := time.Now()
	for i, entry := range entries {
		lines[i] = fmt.Sprintf("id=%d addr=%s laddr=%s name=%s age=%d resp=2 cmd=client|list",
			entry.id, entry.addr, entry.localAddr, entry.name, int(now.Sub(entry.connectedAt).Seconds()))
	}
	return strings.Join(lines, "\n")
}
