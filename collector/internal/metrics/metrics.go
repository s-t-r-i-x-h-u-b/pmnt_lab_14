package metrics

import (
	"runtime"
	"sync"
	"time"
)

// FetchResult records per-country fetch timing and volume.
type FetchResult struct {
	Country        string        `json:"country"`
	DurationMs     float64       `json:"duration_ms"`
	RecordCount    int           `json:"record_count"`
	BytesPublished int64         `json:"bytes_published"`
	MemAllocBytes  uint64        `json:"mem_alloc_bytes"`
	Timestamp      time.Time     `json:"timestamp"`
}

// FlushResult records one tumbling-window flush.
type FlushResult struct {
	RawCount         int       `json:"raw_count"`         // total raw readings aggregated
	AggCount         int       `json:"agg_count"`         // aggregated records emitted
	BytesPublished   int64     `json:"bytes_published"`
	CompressionRatio float64   `json:"compression_ratio"` // raw_count / agg_count
	MemAllocBytes    uint64    `json:"mem_alloc_bytes"`
	Timestamp        time.Time `json:"timestamp"`
}

// Summary is the full metrics snapshot returned by the /metrics endpoint.
type Summary struct {
	Fetches []FetchResult `json:"fetches"`
	Flushes []FlushResult `json:"flushes"`
}

type Collector struct {
	mu           sync.Mutex
	fetchResults []FetchResult
	flushResults []FlushResult
}

func New() *Collector { return &Collector{} }

func (c *Collector) Record(country string, duration time.Duration, records int, bytesPublished int64) FetchResult {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	r := FetchResult{
		Country:        country,
		DurationMs:     float64(duration.Milliseconds()),
		RecordCount:    records,
		BytesPublished: bytesPublished,
		MemAllocBytes:  ms.Alloc,
		Timestamp:      time.Now().UTC(),
	}
	c.mu.Lock()
	c.fetchResults = append(c.fetchResults, r)
	c.mu.Unlock()
	return r
}

func (c *Collector) RecordFlush(rawCount, aggCount int, bytesPublished int64) FlushResult {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	ratio := 0.0
	if aggCount > 0 {
		ratio = float64(rawCount) / float64(aggCount)
	}
	r := FlushResult{
		RawCount:         rawCount,
		AggCount:         aggCount,
		BytesPublished:   bytesPublished,
		CompressionRatio: ratio,
		MemAllocBytes:    ms.Alloc,
		Timestamp:        time.Now().UTC(),
	}
	c.mu.Lock()
	c.flushResults = append(c.flushResults, r)
	c.mu.Unlock()
	return r
}

func (c *Collector) Summary() Summary {
	c.mu.Lock()
	defer c.mu.Unlock()
	fetches := make([]FetchResult, len(c.fetchResults))
	copy(fetches, c.fetchResults)
	flushes := make([]FlushResult, len(c.flushResults))
	copy(flushes, c.flushResults)
	return Summary{Fetches: fetches, Flushes: flushes}
}
