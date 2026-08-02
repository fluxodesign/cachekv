// Command cachekv-server exposes a running cachekv instance over gRPC.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
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
	cacheStoreAddr := flag.String("cachestore-listen", ":6379", "address for the in-memory cache store's plain-text protocol listener")
	persistDb := flag.String("persist-db", "cachestore", "cachekv database name used to persist the in-memory cache store to disk")
	saveInterval := flag.Duration("save-interval", 60*time.Second, "how often to check whether the in-memory cache store should be saved to disk")
	saveMinChanges := flag.Int("save-min-changes", 1, "minimum number of writes since the last save before a save-interval tick actually persists to disk")
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

		cmdChannel = make(chan CkvCommand)
		go stateProcessor(cmdChannel, store, *persistDb, *saveMinChanges)
		go runPeriodicSave(*saveInterval, cmdChannel)

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
				go handleConnection(conn, cmdChannel)
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

// handleConnection deals with network I/O and RESP parsing. Commands are read
// one at a time and funnelled into cmdChan, so replies leave in the same order
// the client sent its requests.
func handleConnection(conn net.Conn, cmdChan chan<- CkvCommand) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v\n", err)
		}
	}(conn)

	reader := newRespReader(conn)
	writer := bufio.NewWriter(conn)
	// One channel for the whole connection: this loop never has more than a
	// single command in flight. Buffered by 1 so the processor doesn't block.
	respChan := make(chan string, 1)

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

		cmd := CkvCommand{
			Op:   strings.ToUpper(args[0]),
			Args: args[1:],
			Resp: respChan,
		}

		// Send command to the single thread, then wait for its response.
		cmdChan <- cmd
		response := <-respChan

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
