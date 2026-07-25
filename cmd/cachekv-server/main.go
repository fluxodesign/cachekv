// Command cachekv-server exposes a running cachekv instance over gRPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
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
	listenAddr := flag.String("listen", ":50051", "address to listen on")
	storePath := flag.String("store-path", cachekv.StorePath, "directory for cachekv's databases")
	keyPath := flag.String("key-path", cachekv.KeyPath, "directory for cachekv's keyring")
	flag.Parse()

	cachekv.StorePath = *storePath
	cachekv.KeyPath = *keyPath

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddr, err)
	}

	cachekv.Startup()

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := cachekv.Shutdown(shutdownCtx, shutdownTimeout); err != nil {
		log.Printf("Shutdown error: %v\n", err)
	} else {
		log.Println("Shutdown complete")
	}
	showMetrics()
	os.Exit(0)
}

func showMetrics() {
	metrics := cachekv.GetMetricsCollector().(*cachekv.SimpleMetricsCollector).GetMetrics()

	fmt.Println("\n=== Final Metrics Report ===")
	fmt.Printf("Total Operations: %d\n", metrics.TotalOperations)
	fmt.Printf("  - Created:     %d\n", metrics.Created)
	fmt.Printf("  - Reads:       %d\n", metrics.Reads)
	fmt.Printf("  - Writes:      %d\n", metrics.Writes)
	fmt.Printf("  - Deletes:     %d\n", metrics.Deletes)
	fmt.Printf("  - Errors:      %d\n", metrics.Errors)
	fmt.Printf("\nActive Databases: %d\n", metrics.DatabasesActive)
	fmt.Printf("Mean Latency:     %v\n", metrics.MeanLatency)
	fmt.Printf("P50 Latency:      %v\n", metrics.P50Latency)
	fmt.Printf("P95 Latency:      %v\n", metrics.P95Latency)
	fmt.Printf("P99 Latency:      %v\n", metrics.P99Latency)
	fmt.Printf("Uptime:           %v\n", metrics.Uptime)
}
