package metrics

import (
	"runtime"
	"sync"
	"time"
)

type FetchResult struct {
	Country        string        `json:"country"`
	Duration       time.Duration `json:"duration_ms"`
	RecordCount    int           `json:"record_count"`
	BytesPublished int64         `json:"bytes_published"`
	MemAllocBytes  uint64        `json:"mem_alloc_bytes"`
	Timestamp      time.Time     `json:"timestamp"`
}

func (r FetchResult) DurationMS() float64 {
	return float64(r.Duration.Milliseconds())
}

type Collector struct {
	mu      sync.Mutex
	results []FetchResult
}

func New() *Collector {
	return &Collector{}
}

func (c *Collector) Record(country string, duration time.Duration, records int, bytesPublished int64) FetchResult {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	r := FetchResult{
		Country:        country,
		Duration:       duration,
		RecordCount:    records,
		BytesPublished: bytesPublished,
		MemAllocBytes:  ms.Alloc,
		Timestamp:      time.Now().UTC(),
	}
	c.mu.Lock()
	c.results = append(c.results, r)
	c.mu.Unlock()
	return r
}

func (c *Collector) Summary() []FetchResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]FetchResult, len(c.results))
	copy(cp, c.results)
	return cp
}
