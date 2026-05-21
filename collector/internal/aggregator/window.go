// Package aggregator implements a tumbling window that groups raw Measurements
// by (countryCode, parameter) and emits statistical aggregates when the window closes.
// A window closes when its timer fires OR when the record buffer reaches MaxSize,
// whichever comes first.
package aggregator

import (
	"math"
	"sync"
	"time"

	"pmnt_lab14/collector/internal/schema"
)

// Config controls when a tumbling window closes.
type Config struct {
	Duration    time.Duration // close on timer tick  (0 = disabled)
	MaxSize     int           // close when buf reaches this size (0 = disabled)
	CollectorID string
}

// AggRecord is one (countryCode, parameter) aggregate produced by a closed window.
type AggRecord struct {
	WindowStart   time.Time
	WindowEnd     time.Time
	CountryCode   string
	Parameter     string
	Unit          string
	Count         int64   // raw readings in this group
	MeanValue     float64
	MinValue      float64
	MaxValue      float64
	StdValue      float64 // population std deviation
	LocationCount int64   // distinct monitoring stations
	CollectorID   string
}

// Window accumulates raw Measurements and closes into aggregated batches.
type Window struct {
	cfg    Config
	mu     sync.Mutex
	buf    []schema.Measurement
	start  time.Time
	out    chan []AggRecord
	stopCh chan struct{}
}

// NewWindow creates and starts a Window.  Callers must call Stop() to release
// the internal timer goroutine.
func NewWindow(cfg Config) *Window {
	w := &Window{
		cfg:    cfg,
		out:    make(chan []AggRecord, 16),
		stopCh: make(chan struct{}),
		start:  time.Now().UTC(),
	}
	if cfg.Duration > 0 {
		go w.timerLoop()
	}
	return w
}

// Flushed returns a receive-only channel that delivers one []AggRecord slice per
// closed window.  The slice is always non-empty.
func (w *Window) Flushed() <-chan []AggRecord { return w.out }

// Add appends measurements to the current window and closes it early when MaxSize
// is reached.
func (w *Window) Add(ms ...schema.Measurement) {
	w.mu.Lock()
	w.buf = append(w.buf, ms...)
	ready := w.cfg.MaxSize > 0 && len(w.buf) >= w.cfg.MaxSize
	w.mu.Unlock()
	if ready {
		w.flush()
	}
}

// Stop shuts down the background timer goroutine.
func (w *Window) Stop() { close(w.stopCh) }

// BufferSize returns the current number of buffered raw records (for diagnostics).
func (w *Window) BufferSize() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buf)
}

func (w *Window) timerLoop() {
	ticker := time.NewTicker(w.cfg.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.flush()
		}
	}
}

func (w *Window) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 {
		w.mu.Unlock()
		return
	}
	buf := w.buf
	winStart := w.start
	w.buf = nil
	w.start = time.Now().UTC()
	w.mu.Unlock()

	records := aggregate(buf, winStart, time.Now().UTC(), w.cfg.CollectorID)
	select {
	case w.out <- records:
	default:
		// drop: consumer is behind — prefer low latency over backpressure here
	}
}

// aggregate groups buf by (countryCode, parameter) and computes per-group stats.
func aggregate(buf []schema.Measurement, winStart, winEnd time.Time, collectorID string) []AggRecord {
	type key struct{ country, param string }
	type accum struct {
		sum, sumSq float64
		min, max   float64
		count      int64
		unit       string
		locs       map[int64]struct{}
	}

	groups := make(map[key]*accum, 64)
	for _, m := range buf {
		k := key{m.CountryCode, m.Parameter}
		g := groups[k]
		if g == nil {
			g = &accum{min: math.MaxFloat64, max: -math.MaxFloat64, locs: make(map[int64]struct{})}
			groups[k] = g
		}
		g.count++
		g.sum += m.Value
		g.sumSq += m.Value * m.Value
		if m.Value < g.min {
			g.min = m.Value
		}
		if m.Value > g.max {
			g.max = m.Value
		}
		g.unit = m.Unit
		g.locs[m.LocationID] = struct{}{}
	}

	records := make([]AggRecord, 0, len(groups))
	for k, g := range groups {
		mean := g.sum / float64(g.count)
		variance := g.sumSq/float64(g.count) - mean*mean
		if variance < 0 {
			variance = 0 // guard against floating-point rounding
		}
		records = append(records, AggRecord{
			WindowStart:   winStart,
			WindowEnd:     winEnd,
			CountryCode:   k.country,
			Parameter:     k.param,
			Unit:          g.unit,
			Count:         g.count,
			MeanValue:     mean,
			MinValue:      g.min,
			MaxValue:      g.max,
			StdValue:      math.Sqrt(variance),
			LocationCount: int64(len(g.locs)),
			CollectorID:   collectorID,
		})
	}
	return records
}
