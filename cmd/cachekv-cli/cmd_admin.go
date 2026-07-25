package main

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

// metricsResult mirrors cachekv.Metrics' JSON field names, with the latency
// fields rendered as duration strings ("1.25ms") to match the text output.
type metricsResult struct {
	TotalOperations uint64 `json:"total_operations"`
	Created         uint64 `json:"created"`
	Reads           uint64 `json:"reads"`
	Writes          uint64 `json:"writes"`
	Deletes         uint64 `json:"deletes"`
	Errors          uint64 `json:"errors"`
	DatabasesActive int64  `json:"databases_active"`
	MeanLatency     string `json:"mean_latency"`
	P50Latency      string `json:"p50_latency"`
	P95Latency      string `json:"p95_latency"`
	P99Latency      string `json:"p99_latency"`
	Uptime          string `json:"uptime"`
}

func cmdPing(e *env, args []string) error {
	if len(args) != 0 {
		return usagef("ping takes no arguments")
	}

	ctx, cancel := e.ctx()
	defer cancel()
	start := time.Now()
	if _, err := e.client.Ping(ctx, &cachekvv1.PingRequest{}); err != nil {
		return err
	}
	latency := time.Since(start)

	return e.emit(pingResult{OK: true, Latency: latency.String()}, func(w io.Writer) {
		fmt.Fprintf(w, "pong (%v)\n", latency)
	})
}

func cmdMetrics(e *env, args []string) error {
	if len(args) != 0 {
		return usagef("metrics takes no arguments")
	}

	ctx, cancel := e.ctx()
	defer cancel()
	resp, err := e.client.GetMetrics(ctx, &cachekvv1.GetMetricsRequest{})
	if err != nil {
		return err
	}

	metrics := metricsResult{
		TotalOperations: resp.GetTotalOperations(),
		Created:         resp.GetCreated(),
		Reads:           resp.GetReads(),
		Writes:          resp.GetWrites(),
		Deletes:         resp.GetDeletes(),
		Errors:          resp.GetErrors(),
		DatabasesActive: resp.GetDatabasesActive(),
		MeanLatency:     resp.GetMeanLatency().AsDuration().String(),
		P50Latency:      resp.GetP50Latency().AsDuration().String(),
		P95Latency:      resp.GetP95Latency().AsDuration().String(),
		P99Latency:      resp.GetP99Latency().AsDuration().String(),
		Uptime:          resp.GetUptime().AsDuration().String(),
	}

	return e.emit(metrics, func(w io.Writer) { printMetrics(w, metrics) })
}

func printMetrics(w io.Writer, metrics metricsResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Total Operations:\t%d\n", metrics.TotalOperations)
	fmt.Fprintf(tw, "  - Created:\t%d\n", metrics.Created)
	fmt.Fprintf(tw, "  - Reads:\t%d\n", metrics.Reads)
	fmt.Fprintf(tw, "  - Writes:\t%d\n", metrics.Writes)
	fmt.Fprintf(tw, "  - Deletes:\t%d\n", metrics.Deletes)
	fmt.Fprintf(tw, "  - Errors:\t%d\n", metrics.Errors)
	fmt.Fprintf(tw, "Active Databases:\t%d\n", metrics.DatabasesActive)
	fmt.Fprintf(tw, "Mean Latency:\t%s\n", metrics.MeanLatency)
	fmt.Fprintf(tw, "P50 Latency:\t%s\n", metrics.P50Latency)
	fmt.Fprintf(tw, "P95 Latency:\t%s\n", metrics.P95Latency)
	fmt.Fprintf(tw, "P99 Latency:\t%s\n", metrics.P99Latency)
	fmt.Fprintf(tw, "Uptime:\t%s\n", metrics.Uptime)
	_ = tw.Flush()
}
