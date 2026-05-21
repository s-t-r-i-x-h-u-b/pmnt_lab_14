// Package kafkaproducer publishes measurements and aggregated windows to Kafka.
// Messages use JSON encoding (one message per Measurement / AggRecord) so that
// Python consumers can process them without an Arrow dependency.
//
// Topics:
//   air.measurements.raw — one JSON object per raw Measurement
//   air.measurements.agg — one JSON object per aggregated AggRecord
//
// Message key = country code → same country always lands on the same partition.
package kafkaproducer

import (
	"context"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"pmnt_lab14/collector/internal/aggregator"
	"pmnt_lab14/collector/internal/schema"
)

const (
	TopicRaw = "air.measurements.raw"
	TopicAgg = "air.measurements.agg"
)

// rawMsg is the JSON payload for one raw measurement.
type rawMsg struct {
	LocationID   int64   `json:"location_id"`
	LocationName string  `json:"location_name"`
	CountryCode  string  `json:"country_code"`
	City         string  `json:"city"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	Parameter    string  `json:"parameter"`
	Value        float64 `json:"value"`
	Unit         string  `json:"unit"`
	TimestampUs  int64   `json:"timestamp_us"`
	CollectorID  string  `json:"collector_id"`
}

// aggMsg is the JSON payload for one aggregated window record.
type aggMsg struct {
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
	CountryCode   string  `json:"country_code"`
	Parameter     string  `json:"parameter"`
	Unit          string  `json:"unit"`
	Count         int64   `json:"count"`
	MeanValue     float64 `json:"mean_value"`
	MinValue      float64 `json:"min_value"`
	MaxValue      float64 `json:"max_value"`
	StdValue      float64 `json:"std_value"`
	LocationCount int64   `json:"location_count"`
	CollectorID   string  `json:"collector_id"`
}

// Producer wraps two kafka.Writer instances (raw + agg topics).
type Producer struct {
	rawWriter *kafka.Writer
	aggWriter *kafka.Writer
	logger    *zap.Logger
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // partition by message key (country code)
		RequiredAcks:           kafka.RequireOne,
		Async:                  true,           // non-blocking; errors logged via logger
		AllowAutoTopicCreation: true,
		WriteTimeout:           10 * time.Second,
	}
}

// New creates a Producer connected to the given Kafka brokers.
func New(brokers []string, logger *zap.Logger) *Producer {
	return &Producer{
		rawWriter: newWriter(brokers, TopicRaw),
		aggWriter: newWriter(brokers, TopicAgg),
		logger:    logger,
	}
}

// PublishRaw serialises each Measurement to JSON and writes to TopicRaw.
// Uses country code as the partition key.
func (p *Producer) PublishRaw(ctx context.Context, measurements []schema.Measurement) {
	if len(measurements) == 0 {
		return
	}
	msgs := make([]kafka.Message, 0, len(measurements))
	for _, m := range measurements {
		b, err := json.Marshal(rawMsg{
			LocationID:   m.LocationID,
			LocationName: m.LocationName,
			CountryCode:  m.CountryCode,
			City:         m.City,
			Latitude:     m.Latitude,
			Longitude:    m.Longitude,
			Parameter:    m.Parameter,
			Value:        m.Value,
			Unit:         m.Unit,
			TimestampUs:  m.Timestamp.UnixMicro(),
			CollectorID:  m.CollectorID,
		})
		if err != nil {
			continue
		}
		msgs = append(msgs, kafka.Message{Key: []byte(m.CountryCode), Value: b})
	}
	if err := p.rawWriter.WriteMessages(ctx, msgs...); err != nil {
		p.logger.Warn("kafka publish raw", zap.Error(err), zap.Int("count", len(msgs)))
	}
}

// PublishAgg serialises each AggRecord to JSON and writes to TopicAgg.
func (p *Producer) PublishAgg(ctx context.Context, records []aggregator.AggRecord) {
	if len(records) == 0 {
		return
	}
	msgs := make([]kafka.Message, 0, len(records))
	for _, r := range records {
		b, err := json.Marshal(aggMsg{
			WindowStart:   r.WindowStart.UTC().Format(time.RFC3339),
			WindowEnd:     r.WindowEnd.UTC().Format(time.RFC3339),
			CountryCode:   r.CountryCode,
			Parameter:     r.Parameter,
			Unit:          r.Unit,
			Count:         r.Count,
			MeanValue:     r.MeanValue,
			MinValue:      r.MinValue,
			MaxValue:      r.MaxValue,
			StdValue:      r.StdValue,
			LocationCount: r.LocationCount,
			CollectorID:   r.CollectorID,
		})
		if err != nil {
			continue
		}
		msgs = append(msgs, kafka.Message{Key: []byte(r.CountryCode), Value: b})
	}
	if err := p.aggWriter.WriteMessages(ctx, msgs...); err != nil {
		p.logger.Warn("kafka publish agg", zap.Error(err), zap.Int("count", len(msgs)))
	}
}

// Close flushes pending messages and closes both writers.
func (p *Producer) Close() {
	_ = p.rawWriter.Close()
	_ = p.aggWriter.Close()
}
