package publisher

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"pmnt_lab14/collector/internal/aggregator"
	"pmnt_lab14/collector/internal/schema"
)

const (
	rawSubjectPrefix = "air.quality."
	aggSubjectPrefix = "air.agg."
)

// Publisher encodes data as Arrow IPC and sends it to NATS.
type Publisher struct {
	nc *nats.Conn
}

func New(nc *nats.Conn) *Publisher {
	return &Publisher{nc: nc}
}

// Publish encodes raw Measurements and publishes to air.quality.<countryCode>.
func (p *Publisher) Publish(_ context.Context, countryCode string, measurements []schema.Measurement) (int64, error) {
	if len(measurements) == 0 {
		return 0, nil
	}
	data, err := schema.EncodeToArrow(measurements)
	if err != nil {
		return 0, fmt.Errorf("encode raw arrow: %w", err)
	}
	if err := p.nc.Publish(rawSubjectPrefix+countryCode, data); err != nil {
		return 0, fmt.Errorf("nats publish raw %s: %w", countryCode, err)
	}
	return int64(len(data)), nil
}

// PublishAgg encodes aggregated records and publishes to air.agg.<countryCode>.
func (p *Publisher) PublishAgg(_ context.Context, countryCode string, records []aggregator.AggRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	data, err := aggregator.EncodeToArrow(records)
	if err != nil {
		return 0, fmt.Errorf("encode agg arrow: %w", err)
	}
	if err := p.nc.Publish(aggSubjectPrefix+countryCode, data); err != nil {
		return 0, fmt.Errorf("nats publish agg %s: %w", countryCode, err)
	}
	return int64(len(data)), nil
}
