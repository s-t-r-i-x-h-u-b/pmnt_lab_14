package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"pmnt_lab14/collector/internal/flightserver"
	"pmnt_lab14/collector/internal/metrics"
	"pmnt_lab14/collector/internal/publisher"
	"pmnt_lab14/collector/internal/schema"
	"pmnt_lab14/collector/internal/validator"
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
	fetchInterval  := parseDuration(getenv("FETCH_INTERVAL",  "5m"),  5*time.Minute)
	metricsAddr    := getenv("METRICS_ADDR", ":8080")
	windowDuration := parseDuration(getenv("WINDOW_DURATION", "60s"), 60*time.Second)
	windowMaxSize  := parseInt(getenv("WINDOW_MAX_SIZE", "500"), 500)
	publishRaw     := getenv("PUBLISH_RAW", "true") == "true"
	flightAddr     := getenv("FLIGHT_ADDR", ":5005")
	flightStoreLen := parseInt(getenv("FLIGHT_STORE_LEN", "200"), 200)

	logger.Info("Starting collector",
		zap.String("id", instanceID),
		zap.Strings("etcd", etcdEndpoints),
		zap.String("nats", natsURL),
		zap.Duration("fetch_interval", fetchInterval),
		zap.Duration("window_duration", windowDuration),
		zap.Int("window_max_size", windowMaxSize),
		zap.Bool("publish_raw", publishRaw),
		zap.String("flight_addr", flightAddr),
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

	// Arrow Flight server.
	flightSrv := flightserver.New(flightStoreLen, logger)
	if err := flightSrv.Start(flightAddr); err != nil {
		logger.Fatal("Start Flight server", zap.Error(err))
	}
	defer flightSrv.Stop()

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
		flightSrv:  flightSrv,
		publishRaw: publishRaw,
		logger:     logger,
	}
	coord.SetCallbacks(app.OnShardAssigned, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx, allShards); err != nil {
		logger.Fatal("Start coordinator", zap.Error(err))
	}

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
		rawLen, aggLen := flightSrv.Sizes()
		json.NewEncoder(w).Encode(map[string]any{
			"buffer_size":        win.BufferSize(),
			"window_duration":    windowDuration.String(),
			"window_max_size":    windowMaxSize,
			"flight_raw_batches": rawLen,
			"flight_agg_batches": aggLen,
		})
	})
	// Prometheus text-format metrics — used by Prometheus scraper and HPA custom metrics.
	http.HandleFunc("/metrics/prometheus", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		bufSize := win.BufferSize()
		rawLen, aggLen := flightSrv.Sizes()
		summary := app.metrics.Summary()
		var fetchTotal, bytesTotal int64
		for _, f := range summary.Fetches {
			fetchTotal++
			bytesTotal += f.BytesPublished
		}
		var flushTotal int64
		for range summary.Flushes {
			flushTotal++
		}
		id := instanceID
		fmt.Fprintf(w, "# HELP collector_window_buffer_size Measurements buffered in the tumbling window.\n")
		fmt.Fprintf(w, "# TYPE collector_window_buffer_size gauge\n")
		fmt.Fprintf(w, "collector_window_buffer_size{collector_id=%q} %d\n", id, bufSize)
		fmt.Fprintf(w, "# HELP collector_flight_raw_batches Arrow record batches in the raw Flight ring-buffer.\n")
		fmt.Fprintf(w, "# TYPE collector_flight_raw_batches gauge\n")
		fmt.Fprintf(w, "collector_flight_raw_batches{collector_id=%q} %d\n", id, rawLen)
		fmt.Fprintf(w, "# HELP collector_flight_agg_batches Arrow record batches in the agg Flight ring-buffer.\n")
		fmt.Fprintf(w, "# TYPE collector_flight_agg_batches gauge\n")
		fmt.Fprintf(w, "collector_flight_agg_batches{collector_id=%q} %d\n", id, aggLen)
		fmt.Fprintf(w, "# HELP collector_fetches_total Total OpenAQ fetch cycles completed.\n")
		fmt.Fprintf(w, "# TYPE collector_fetches_total counter\n")
		fmt.Fprintf(w, "collector_fetches_total{collector_id=%q} %d\n", id, fetchTotal)
		fmt.Fprintf(w, "# HELP collector_bytes_published_total Total bytes published to NATS.\n")
		fmt.Fprintf(w, "# TYPE collector_bytes_published_total counter\n")
		fmt.Fprintf(w, "collector_bytes_published_total{collector_id=%q} %d\n", id, bytesTotal)
		fmt.Fprintf(w, "# HELP collector_window_flushes_total Tumbling window flush events.\n")
		fmt.Fprintf(w, "# TYPE collector_window_flushes_total counter\n")
		fmt.Fprintf(w, "collector_window_flushes_total{collector_id=%q} %d\n", id, flushTotal)
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
	flightSrv  *flightserver.FlightServer
	publishRaw bool
	logger     *zap.Logger
}

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

// fetchAndPublish fetches raw measurements, publishes to NATS (if enabled),
// feeds the Flight store, and enqueues into the tumbling window.
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

	// Validate measurements via Rust library (no-op stub when not linked).
	measurements, invalidCount := validator.FilterMeasurements(measurements)
	if invalidCount > 0 {
		a.logger.Info("validation_filter",
			zap.String("country", country),
			zap.Int("valid", len(measurements)),
			zap.Int("invalid", invalidCount),
		)
	}
	if len(measurements) == 0 {
		return
	}

	// Build one Arrow Record shared by Flight and (optionally) NATS encode.
	rawRec, err := schema.BuildRecord(measurements)
	if err != nil {
		a.logger.Error("BuildRecord error", zap.String("country", country), zap.Error(err))
		return
	}
	defer rawRec.Release()

	// Feed the Flight server directly from the in-memory Record.
	a.flightSrv.AddRaw(rawRec)

	// Optionally publish raw data to NATS (Arrow IPC re-encoded from the Record).
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

	a.window.Add(measurements...)
}

// publishAggregated groups AggRecords by country, publishes via NATS, and feeds
// the aggregated Flight store.
func (a *App) publishAggregated(ctx context.Context, records []aggregator.AggRecord) {
	// Build one Arrow Record for the Flight store.
	aggRec, err := aggregator.BuildRecord(records)
	if err == nil {
		a.flightSrv.AddAgg(aggRec)
		aggRec.Release()
	}

	byCountry := make(map[string][]aggregator.AggRecord, 8)
	for _, r := range records {
		byCountry[r.CountryCode] = append(byCountry[r.CountryCode], r)
	}

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
