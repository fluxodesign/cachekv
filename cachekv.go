package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fluxodesign/cachekv/cachekv"
)

func main() {
	// Set up graceful shutdown signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cachekv.Startup()

	// wait for shutdown signal
	select {
	case <-sigChan:
		log.Println("Received shutdown signal, initiating shutdown...")
		showMetrics()
		e := cachekv.Shutdown(shutdownCtx, 30*time.Second)
		if e != nil {
			log.Printf("Shutdown error: %v\n", e)
		} else {
			log.Println("Shutdown complete")
		}
	}
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
	fmt.Printf("Avg Latency:      %v\n", metrics.AverageLatency)
	fmt.Printf("Uptime:           %v\n", metrics.Uptime)
}
