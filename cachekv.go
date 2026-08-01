package main

import (
	"context"
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
		cachekv.ShowMetrics()
		e := cachekv.Shutdown(shutdownCtx, 30*time.Second)
		if e != nil {
			log.Printf("Shutdown error: %v\n", e)
		} else {
			log.Println("Shutdown complete")
		}
	}
	os.Exit(0)
}
