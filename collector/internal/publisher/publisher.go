package publisher

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"pmnt_lab14/collector/internal/schema"
)

const subjectPrefix = "air.quality."

// Publisher encodes Measurements as Arrow IPC and sends them to NATS.
type Publisher struct {
	nc *nats.Conn
}

func New(nc *nats.Conn) *Publisher {
	return &Publisher{nc: nc}
}

// Publish encodes the batch and publishes it to air.quality.<countryCode>.
// Returns the number of bytes published.
func (p *Publisher) Publish(_ context.Context, countryCode string, measurements []schema.Measurement) (int64, error) {
	if len(measurements) == 0 {
		return 0, nil
	}
	data, err := schema.EncodeToArrow(measurements)
	if err != nil {
		return 0, fmt.Errorf("encode arrow: %w", err)
	}
	subject := subjectPrefix + countryCode
	if err := p.nc.Publish(subject, data); err != nil {
		return 0, fmt.Errorf("nats publish %s: %w", subject, err)
	}
	return int64(len(data)), nil
}
