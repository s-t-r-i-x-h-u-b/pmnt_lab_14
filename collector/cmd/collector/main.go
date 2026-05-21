package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"pmnt_lab14/collector/internal/coordinator"
	"pmnt_lab14/collector/internal/fetcher"
	"pmnt_lab14/collector/internal/metrics"
	"pmnt_lab14/collector/internal/publisher"
)

// allShards is the universe of country codes. The leader distributes these across collectors.
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
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	instanceID := getenv("INSTANCE_ID", uuid.New().String())
	etcdEndpoints := strings.Split(getenv("ETCD_ENDPOINTS", "http://localhost:2379"), ",")
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	openAQKey := getenv("OPENAQ_API_KEY", "")
	fetchInterval := parseDuration(getenv("FETCH_INTERVAL", "5m"), 5*time.Minute)
	metricsAddr := getenv("METRICS_ADDR", ":8080")

	logger.Info("Starting collector",
		zap.String("id", instanceID),
		zap.Strings("etcd", etcdEndpoints),
		zap.String("nats", natsURL),
		zap.Duration("fetch_interval", fetchInterval),
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

	app := &App{
		id:        instanceID,
		fetcher:   fetcher.NewClient(openAQKey),
		publisher: publisher.New(nc),
		metrics:   metrics.New(),
		interval:  fetchInterval,
		logger:    logger,
	}
	coord.SetCallbacks(app.OnShardAssigned, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx, allShards); err != nil {
		logger.Fatal("Start coordinator", zap.Error(err))
	}

	// Expose metrics over HTTP.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(app.metrics.Summary())
	})
	http.HandleFunc("/shards", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(coord.AssignedShards())
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

// App holds per-collector state and implements the shard-assigned callback.
type App struct {
	id        string
	fetcher   *fetcher.Client
	publisher *publisher.Publisher
	metrics   *metrics.Collector
	interval  time.Duration
	logger    *zap.Logger
}

// OnShardAssigned is called by the coordinator when a new shard is assigned to this instance.
// It immediately fetches data and then repeats on every interval tick until ctx is cancelled.
func (a *App) OnShardAssigned(ctx context.Context, shard string) {
	a.logger.Info("Fetching shard", zap.String("country", shard))
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

	bytesPublished, err := a.publisher.Publish(ctx, country, measurements)
	if err != nil {
		a.logger.Error("Publish error", zap.String("country", country), zap.Error(err))
		return
	}

	r := a.metrics.Record(country, time.Since(start), len(measurements), bytesPublished)
	a.logger.Info("fetch_complete",
		zap.String("country", country),
		zap.Float64("duration_ms", r.DurationMS()),
		zap.Int("records", r.RecordCount),
		zap.Int64("bytes_published", r.BytesPublished),
		zap.Uint64("mem_alloc_bytes", r.MemAllocBytes),
	)
}
