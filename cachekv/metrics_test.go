package cachekv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLatencyStatsMeanAndPercentiles(t *testing.T) {
	var l latencyStats
	// 1ms..100ms in 1ms steps
	for i := 1; i <= 100; i++ {
		l.record(time.Duration(i) * time.Millisecond)
	}

	assert.Equal(t, 50500*time.Microsecond, l.mean()) // (1+...+100)/100 = 50.5ms

	p50, p95, p99 := l.percentiles()
	assert.Equal(t, 50*time.Millisecond, p50)
	assert.Equal(t, 95*time.Millisecond, p95)
	assert.Equal(t, 99*time.Millisecond, p99)
}

func TestLatencyStatsEmpty(t *testing.T) {
	var l latencyStats
	assert.Equal(t, time.Duration(0), l.mean())
	p50, p95, p99 := l.percentiles()
	assert.Equal(t, time.Duration(0), p50)
	assert.Equal(t, time.Duration(0), p95)
	assert.Equal(t, time.Duration(0), p99)
}

func TestLatencyStatsRingWraparoundKeepsWindowedPercentiles(t *testing.T) {
	var l latencyStats
	// Fill the ring, then push it past capacity with a distinct high value so we
	// can confirm older samples fall out of the percentile window.
	for i := 0; i < latencyRingSize; i++ {
		l.record(time.Millisecond)
	}
	for i := 0; i < latencyRingSize; i++ {
		l.record(time.Second)
	}

	// Mean covers the entire lifetime, so it reflects both windows.
	wantMean := time.Duration((int64(latencyRingSize)*time.Millisecond.Nanoseconds() +
		int64(latencyRingSize)*time.Second.Nanoseconds()) / (2 * int64(latencyRingSize)))
	assert.Equal(t, wantMean, l.mean())

	// Percentiles only see the most recent window, all 1s samples.
	p50, p95, p99 := l.percentiles()
	assert.Equal(t, time.Second, p50)
	assert.Equal(t, time.Second, p95)
	assert.Equal(t, time.Second, p99)
}

func TestLatencyStatsConcurrentRecord(t *testing.T) {
	var l latencyStats
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			l.record(time.Duration(n+1) * time.Millisecond)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint64(200), l.count.Load())
	assert.True(t, l.mean() > 0)
}

func TestSimpleMetricsCollectorReportsDistribution(t *testing.T) {
	m := NewSimpleMetricsCollector()
	ctx := context.Background()
	for i := 1; i <= 100; i++ {
		m.RecordOperation(ctx, "read", "testdb", time.Duration(i)*time.Millisecond, true)
	}

	metrics := m.GetMetrics()
	assert.Equal(t, uint64(100), metrics.Reads)
	assert.Equal(t, 50500*time.Microsecond, metrics.MeanLatency)
	assert.Equal(t, 50*time.Millisecond, metrics.P50Latency)
	assert.Equal(t, 95*time.Millisecond, metrics.P95Latency)
	assert.Equal(t, 99*time.Millisecond, metrics.P99Latency)
}
