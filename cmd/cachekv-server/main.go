// Command cachekv-server exposes a running cachekv instance over gRPC.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/fluxodesign/cachekv/cachekv"
	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
	"github.com/fluxodesign/cachekv/internal/grpcserver"
)

const (
	shutdownTimeout = 30 * time.Second
	drainTimeout    = 25 * time.Second
)

func main() {
	runAsCachestore := flag.Bool("run-as-cachestore", true, "if true, run as in-memory cache store")
	listenAddr := flag.String("listen", ":50051", "address to listen on")
	cacheStoreAddr := flag.String("cachestore-listen", "127.0.0.1:6379", "address for the in-memory cache store's plain-text protocol listener; loopback-only by default since it has no TLS — widen deliberately with --requirepass set")
	requirePass := flag.String("requirepass", "", "if set, clients on the cache store's raw listener must AUTH with this password before running any other command")
	persistDb := flag.String("persist-db", "cachestore", "cachekv database name used to persist the in-memory cache store to disk")
	saveInterval := flag.Duration("save-interval", 60*time.Second, "how often to check whether the in-memory cache store should be saved to disk")
	saveMinChanges := flag.Int("save-min-changes", 1, "minimum number of writes since the last save before a save-interval tick actually persists to disk")
	activeExpireInterval := flag.Duration("active-expire-interval", time.Second, "how often to sweep the cache store for keys past their TTL, so an untouched expired key doesn't sit in memory (and get re-persisted) forever")
	storePath := flag.String("store-path", cachekv.StorePath, "directory for cachekv's databases")
	keyPath := flag.String("key-path", cachekv.KeyPath, "directory for cachekv's keyring")
	onKeyCollision := flag.String("on-key-collision", collisionPolicyWarn, "policy when a persisted key collides across types on load: warn (log and purge the losing values) or fail (log and refuse to start)")
	flag.Parse()

	if *onKeyCollision != collisionPolicyWarn && *onKeyCollision != collisionPolicyFail {
		log.Fatalf("invalid --on-key-collision %q: must be %q or %q", *onKeyCollision, collisionPolicyWarn, collisionPolicyFail)
	}

	cachekv.StorePath = *storePath
	cachekv.KeyPath = *keyPath

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddr, err)
	}

	cachekv.Startup()

	// Declared here (rather than inside the block below) so the shutdown
	// sequence can also reach it to force a final save.
	var cmdChannel chan CkvCommand
	if *runAsCachestore {
		cacheStoreLis, err := net.Listen("tcp", *cacheStoreAddr)
		if err != nil {
			log.Fatalf("failed to listen on %s: %v", *cacheStoreAddr, err)
		}

		if err := cachekv.CreateDatabase(*persistDb, false); err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("failed to create persist database %s: %v", *persistDb, err)
		}
		store, err := loadFromDisk(*persistDb, *onKeyCollision)
		if err != nil {
			log.Printf("failed to load persisted cache store %s, starting empty: %v\n", *persistDb, err)
			store = NewDatastore()
		}
		// CONFIG GET save should report the rule actually in effect, not the
		// static placeholder NewDatastore() seeds it with.
		store.config["save"] = fmt.Sprintf("%d %d", int(saveInterval.Seconds()), *saveMinChanges)

		cmdChannel = make(chan CkvCommand)
		go stateProcessor(cmdChannel, store, *persistDb, *saveMinChanges)
		go runPeriodicSave(*saveInterval, cmdChannel)
		go runActiveExpire(*activeExpireInterval, cmdChannel)

		registry := newClientRegistry()

		go func() {
			log.Printf("cachekv-server (cache store) listening on %s\n", *cacheStoreAddr)
			for {
				conn, err := cacheStoreLis.Accept()
				if err != nil {
					log.Printf("cache store listener stopped accepting: %v\n", err)
					return
				}
				// Each connection gets its own goroutine so slow or idle
				// clients never block others; all of them funnel commands
				// into the single cmdChannel processed by stateProcessor.
				go handleConnection(conn, cmdChannel, *requirePass, registry, func() {
					// SHUTDOWN reuses the exact graceful path a real SIGTERM
					// already triggers below (grpc drain -> forced save ->
					// cachekv.Shutdown), rather than duplicating it.
					_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				})
			}
		}()
	}

	grpcServer := grpc.NewServer()
	cachekvv1.RegisterCacheKVServer(grpcServer, grpcserver.New())
	healthServer := grpcserver.NewHealthServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		log.Printf("cachekv-server listening on %s\n", *listenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc server stopped serving: %v\n", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan
	log.Println("Received shutdown signal, initiating shutdown...")

	// Stop routing new traffic here, then drain in-flight RPCs before touching
	// cachekv's connection pool (see plan: Shutdown ordering).
	grpcserver.SetNotServing(healthServer)

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(drainTimeout):
		log.Println("Graceful drain timed out, forcing gRPC server stop")
		grpcServer.Stop()
	}

	// Force a final save before cachekv.Shutdown closes the connection pool —
	// BatchInsert would race/fail against a closing pool afterwards.
	if *runAsCachestore {
		resp := make(chan string, 1)
		cmdChannel <- CkvCommand{Op: "SAVE", Args: []string{"force"}, Resp: resp}
		if ack := <-resp; strings.HasPrefix(ack, "-ERR") {
			log.Printf("final cache store save failed: %s", ack)
		} else {
			log.Println("Cache store saved to disk")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := cachekv.Shutdown(shutdownCtx, shutdownTimeout); err != nil {
		log.Printf("Shutdown error: %v\n", err)
	} else {
		log.Println("Shutdown complete")
	}
	cachekv.ShowMetrics()
	os.Exit(0)
}

// connState is per-connection state that neither Datastore nor stateProcessor
// have any business tracking: authentication, the connection's registry id,
// and its CLIENT SETNAME name. Shared by handleAuth/handleHello/handleClient.
type connState struct {
	authenticated bool
	id            int64
	name          string
}

// handleConnection deals with network I/O and RESP parsing. Commands are read
// one at a time and funnelled into cmdChan, so replies leave in the same order
// the client sent its requests.
//
// requirePass, when non-empty, gates every command but AUTH/HELLO/QUIT behind
// a per-connection authenticated flag — the same minimal pre-auth allowlist
// real Redis itself uses. AUTH, HELLO and CLIENT are all handled entirely
// here rather than via commandRegistry (they never touch cmdChan/Datastore),
// since they need either per-connection state the single shared
// stateProcessor goroutine has no business tracking, or (QUIT, SHUTDOWN)
// effects on the connection/process that a Datastore command can't express.
func handleConnection(conn net.Conn, cmdChan chan<- CkvCommand, requirePass string, registry *clientRegistry, triggerShutdown func()) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v\n", err)
		}
	}(conn)

	entry := registry.register(conn.RemoteAddr().String(), conn.LocalAddr().String())
	defer registry.unregister(entry.id)

	reader := newRespReader(conn)
	writer := bufio.NewWriter(conn)
	// One channel for the whole connection: this loop never has more than a
	// single command in flight. Buffered by 1 so the processor doesn't block.
	respChan := make(chan string, 1)

	state := &connState{authenticated: requirePass == "", id: entry.id}

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			// On a protocol error the stream is out of sync, so tell the
			// client what happened and hang up. Everything else (EOF, reset)
			// means the connection is already gone.
			var protoErr protocolError
			switch {
			case errors.As(err, &protoErr):
				_, _ = fmt.Fprintf(writer, "-ERR Protocol error: %s\r\n", protoErr)
				_ = writer.Flush()
			case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
			default:
				log.Printf("Error reading from %s: %v\n", conn.RemoteAddr(), err)
			}
			return
		}

		op := strings.ToUpper(args[0])

		var response string
		switch {
		case op == "AUTH":
			response = handleAuth(requirePass, args[1:], state)
		case op == "HELLO":
			response = handleHello(requirePass, args[1:], state)
		case op == "QUIT":
			_, _ = writer.WriteString(okReply)
			_ = writer.Flush()
			return
		case op == "RESET":
			response = handleReset(requirePass, state, registry)
		case !state.authenticated:
			response = errNoAuth
		case op == "CLIENT":
			response = handleClient(args[1:], state, registry)
		case op == "SHUTDOWN":
			log.Printf("cache store: SHUTDOWN received from %s, triggering graceful shutdown", conn.RemoteAddr())
			triggerShutdown()
			return
		default:
			cmd := CkvCommand{Op: op, Args: args[1:], Resp: respChan}
			// Send command to the single thread, then wait for its response.
			cmdChan <- cmd
			response = <-respChan
		}

		if _, err := writer.WriteString(response); err != nil {
			log.Printf("Error writing response to connection: %v\n", err)
			return
		}
		// Hold the reply back only while further pipelined commands are
		// already buffered, so a batch leaves in one write while an
		// interactive client still gets an immediate answer.
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				log.Printf("Error flushing response to connection: %v\n", err)
				return
			}
		}
	}
}

// handleAuth implements the AUTH command, run before commandRegistry and
// without ever touching cmdChan — see handleConnection's doc comment for why.
func handleAuth(requirePass string, args []string, state *connState) string {
	if requirePass == "" {
		return "-ERR Client sent AUTH, but no password is set.\r\n"
	}
	if len(args) != 1 {
		return wrongArgs("auth")
	}
	if passwordsEqual(args[0], requirePass) {
		state.authenticated = true
		return okReply
	}
	// A previously-authenticated connection that sends a wrong password is
	// re-gated: AUTH always re-validates, it doesn't just check once.
	state.authenticated = false
	return "-ERR invalid password\r\n"
}

// handleHello implements HELLO [protover] [AUTH password] [SETNAME name].
// Real RESP3 (maps, doubles, booleans, push types) isn't implemented
// anywhere in resp.go, so whatever protover the client asks for, the reply
// always reports proto 2 — a client that respects that field falls back to
// RESP2 parsing, which is all this server ever writes.
// handleReset implements RESET: connection-plane like AUTH/HELLO/CLIENT,
// pre-auth exempt like QUIT/HELLO (matches real Redis). Only resets what's
// actually meaningful today — authentication and the CLIENT SETNAME name —
// since MULTI/WATCH/pub-sub don't exist yet to reset.
func handleReset(requirePass string, state *connState, registry *clientRegistry) string {
	state.authenticated = requirePass == ""
	state.name = ""
	registry.setName(state.id, "")
	return "+RESET\r\n"
}

func handleHello(requirePass string, args []string, state *connState) string {
	i := 0
	if i < len(args) {
		if _, err := strconv.Atoi(args[i]); err != nil || (args[i] != "2" && args[i] != "3") {
			return "-NOPROTO unsupported protocol version\r\n"
		}
		i++
	}

	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "AUTH":
			if i+2 >= len(args) {
				return wrongArgs("hello")
			}
			// Username is accepted and ignored: one shared password, no ACL
			// — same model plain AUTH already uses.
			if requirePass == "" || !passwordsEqual(args[i+2], requirePass) {
				return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
			}
			state.authenticated = true
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				return wrongArgs("hello")
			}
			state.name = args[i+1]
			i += 2
		default:
			return errSyntax
		}
	}

	if requirePass != "" && !state.authenticated {
		return errNoAuth
	}

	return arrayReply(
		bulkString("server"), bulkString("cachekv"),
		bulkString("version"), bulkString("0.1.0"),
		bulkString("proto"), integer(2),
		bulkString("id"), integer(int(state.id)),
		bulkString("mode"), bulkString("standalone"),
		bulkString("role"), bulkString("master"),
		bulkString("modules"), emptyArray,
	)
}

// handleClient implements CLIENT ID/GETNAME/SETNAME/LIST.
func handleClient(args []string, state *connState, registry *clientRegistry) string {
	if len(args) == 0 {
		return wrongArgs("client")
	}
	switch strings.ToUpper(args[0]) {
	case "ID":
		return integer(int(state.id))
	case "GETNAME":
		return bulkString(state.name)
	case "SETNAME":
		if len(args) != 2 {
			return wrongArgs("client|setname")
		}
		name := args[1]
		if strings.ContainsAny(name, " \n\r") {
			return "-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
		}
		state.name = name
		registry.setName(state.id, name)
		return okReply
	case "LIST":
		if len(args) != 1 {
			// Redis's ID/TYPE filters aren't supported — fail loud rather
			// than silently returning an unfiltered list.
			return errSyntax
		}
		return bulkString(registry.list())
	default:
		return fmt.Sprintf("-ERR Unknown CLIENT subcommand or wrong number of arguments for '%s'\r\n", args[0])
	}
}

// passwordsEqual reports whether a and b match, comparing in constant time
// over a fixed-size hash of each so that neither a length mismatch nor a
// matching prefix leaks timing information.
func passwordsEqual(a, b string) bool {
	sumA := sha256.Sum256([]byte(a))
	sumB := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(sumA[:], sumB[:]) == 1
}
