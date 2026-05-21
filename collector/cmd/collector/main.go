package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"pmnt_lab14/collector/internal/aggregator"
	"pmnt_lab14/collector/internal/coordinator"
	"pmnt_lab14/collector/internal/fetcher"
	"pmnt_lab14/collector/internal/metrics"
	"pmnt_lab14/collector/internal/publisher"
)

var allShards = []string{
	"US", "GB", "DE", "FR", "PL", "NL", "IN", "AU", "CA", "BR",
	"JP", "KR", "ZA", "MX", "IT", "ES", "SE", "NO", "DK", "CZ",
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseDuration(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}

func parseInt(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	instanceID     := getenv("INSTANCE_ID", uuid.New().String())
	etcdEndpoints  := strings.Split(getenv("ETCD_ENDPOINTS", "http://localhost:2379"), ",")
	natsURL        := getenv("NATS_URL", nats.DefaultURL)
	openAQKey      := getenv("OPENAQ_API_KEY", "")
	fetchInterval  := parseDuration(getenv("FETCH_INTERVAL", "5m"), 5*time.Minute)
	metricsAddr    := getenv("METRICS_ADDR", ":8080")
	windowDuration := parseDuration(getenv("WINDOW_DURATION", "60s"), 60*time.Second)
	windowMaxSize  := parseInt(getenv("WINDOW_MAX_SIZE", "500"), 500)
	publishRaw     := getenv("PUBLISH_RAW", "true") == "true"

	logger.Info("Starting collector",
		zap.String("id", instanceID),
		zap.Strings("etcd", etcdEndpoints),
		zap.String("nats", natsURL),
		zap.Duration("fetch_interval", fetchInterval),
		zap.Duration("window_duration", windowDuration),
		zap.Int("window_max_size", windowMaxSize),
		zap.Bool("publish_raw", publishRaw),
	)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatal("Connect to etcd", zap.Error(err))
	}
	defer etcdCli.Close()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatal("Connect to NATS", zap.Error(err))
	}
	defer nc.Drain()

	host, _ := os.Hostname()
	coord := coordinator.New(etcdCli, instanceID, coordinator.Info{
		ID:        instanceID,
		StartedAt: time.Now().UTC(),
		Host:      host,
	}, logger)

	win := aggregator.NewWindow(aggregator.Config{
		Duration:    windowDuration,
		MaxSize:     windowMaxSize,
		CollectorID: instanceID,
	})

	app := &App{
		id:         instanceID,
		fetcher:    fetcher.NewClient(openAQKey),
		publisher:  publisher.New(nc),
		metrics:    metrics.New(),
		interval:   fetchInterval,
		window:     win,
		publishRaw: publishRaw,
		logger:     logger,
	}
	coord.SetCallbacks(app.OnShardAssigned, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx, allShards); err != nil {
		logger.Fatal("Start coordinator", zap.Error(err))
	}

	// Flush goroutine: reads closed windows and publishes aggregated batches.
	go func() {
		for {
			select {
			case <-ctx.Done():
				win.Stop()
				return
			case batch := <-win.Flushed():
				app.publishAggregated(ctx, batch)
			}
		}
	}()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(app.metrics.Summary())
	})
	http.HandleFunc("/shards", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(coord.AssignedShards())
	})
	http.HandleFunc("/window", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"buffer_size":     win.BufferSize(),
			"window_duration": windowDuration.String(),
			"window_max_size": windowMaxSize,
		})
	})
	srv := &http.Server{Addr: metricsAddr}
	go func() { _ = srv.ListenAndServe() }()
	logger.Info("Metrics HTTP server", zap.String("addr", metricsAddr))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("Shutting down")
	cancel()
	coord.Stop()
	_ = srv.Shutdown(context.Background())
}

// App holds per-collector state.
type App struct {
	id         string
	fetcher    *fetcher.Client
	publisher  *publisher.Publisher
	metrics    *metrics.Collector
	interval   time.Duration
	window     *aggregator.Window
	publishRaw bool
	logger     *zap.Logger
}

// OnShardAssigned fetches data for the given country on every interval tick.
func (a *App) OnShardAssigned(ctx context.Context, shard string) {
	a.fetchAndPublish(ctx, shard)

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.fetchAndPublish(ctx, shard)
		}
	}
}

// fetchAndPublish fetches raw measurements, optionally publishes them, then feeds
// them into the tumbling window for aggregation.
func (a *App) fetchAndPublish(ctx context.Context, country string) {
	start := time.Now()

	measurements, err := a.fetcher.FetchMeasurements(ctx, country, a.id)
	if err != nil {
		a.logger.Warn("Fetch error", zap.String("country", country), zap.Error(err))
		return
	}
	if len(measurements) == 0 {
		return
	}

	fetchDuration := time.Since(start)

	// Optionally publish raw data.
	var bytesRaw int64
	if a.publishRaw {
		bytesRaw, err = a.publisher.Publish(ctx, country, measurements)
		if err != nil {
			a.logger.Error("Publish raw error", zap.String("country", country), zap.Error(err))
		}
		r := a.metrics.Record(country, fetchDuration, len(measurements), bytesRaw)
		a.logger.Info("fetch_complete",
			zap.String("country", country),
			zap.Float64("duration_ms", r.DurationMs),
			zap.Int("records", r.RecordCount),
			zap.Int64("bytes_raw", r.BytesPublished),
			zap.Uint64("mem_alloc_bytes", r.MemAllocBytes),
		)
	} else {
		a.logger.Info("fetch_complete",
			zap.String("country", country),
			zap.Float64("duration_ms", float64(fetchDuration.Milliseconds())),
			zap.Int("records", len(measurements)),
		)
	}

	// Always feed into the tumbling window.
	a.window.Add(measurements...)
}

// publishAggregated groups AggRecords by country and publishes each group.
func (a *App) publishAggregated(ctx context.Context, records []aggregator.AggRecord) {
	byCountry := make(map[string][]aggregator.AggRecord, 8)
	for _, r := range records {
		byCountry[r.CountryCode] = append(byCountry[r.CountryCode], r)
	}

	// Sum raw readings represented in this window for the compression metric.
	var rawCount int
	for _, r := range records {
		rawCount += int(r.Count)
	}

	var totalBytes int64
	for country, recs := range byCountry {
		b, err := a.publisher.PublishAgg(ctx, country, recs)
		if err != nil {
			a.logger.Error("PublishAgg error", zap.String("country", country), zap.Error(err))
			continue
		}
		totalBytes += b
	}

	r := a.metrics.RecordFlush(rawCount, len(records), totalBytes)
	a.logger.Info("window_flush",
		zap.Int("raw_records_in", r.RawCount),
		zap.Int("agg_records_out", r.AggCount),
		zap.Int64("bytes_published", r.BytesPublished),
		zap.Float64("compression_ratio", r.CompressionRatio),
		zap.Uint64("mem_alloc_bytes", r.MemAllocBytes),
	)
}
