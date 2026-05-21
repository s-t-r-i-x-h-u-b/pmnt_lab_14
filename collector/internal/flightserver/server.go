// Package flightserver embeds an Apache Arrow Flight gRPC server into the Go collector.
// Python (or any Arrow-capable) clients connect directly and stream RecordBatches
// without going through NATS, giving zero-copy columnar access to collected data.
package flightserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"go.uber.org/zap"

	"pmnt_lab14/collector/internal/aggregator"
	"pmnt_lab14/collector/internal/schema"
)

// TicketRequest is the JSON payload inside a Flight Ticket.
// Dataset is "raw" or "agg"; Country and Parameter are optional server-side filters.
type TicketRequest struct {
	Dataset   string `json:"dataset"`
	Country   string `json:"country,omitempty"`
	Parameter string `json:"parameter,omitempty"`
}

// ─── In-memory record store ───────────────────────────────────────────────────

// recordStore is a bounded ring-buffer of Arrow Records.
type recordStore struct {
	mu      sync.RWMutex
	schema  *arrow.Schema
	records []arrow.Record
	maxLen  int
}

func newRecordStore(s *arrow.Schema, maxLen int) *recordStore {
	return &recordStore{schema: s, maxLen: maxLen}
}

// add appends a record, retaining ownership, and evicts the oldest when full.
func (s *recordStore) add(rec arrow.Record) {
	rec.Retain()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	for len(s.records) > s.maxLen {
		s.records[0].Release()
		s.records = s.records[1:]
	}
}

// snapshot returns a retained slice of all current records.
// Caller must Release() each entry when done.
func (s *recordStore) snapshot() []arrow.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]arrow.Record, len(s.records))
	for i, r := range s.records {
		r.Retain()
		out[i] = r
	}
	return out
}

func (s *recordStore) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

// ─── Flight server ────────────────────────────────────────────────────────────

// FlightServer is an Arrow Flight gRPC server embedded in the Go collector.
// It exposes two datasets: "raw" (Measurements) and "agg" (AggRecords).
type FlightServer struct {
	flight.BaseFlightServer
	rawStore *recordStore
	aggStore *recordStore
	logger   *zap.Logger
	srv      flight.Server
}

// New creates a FlightServer with a bounded store of storeLen RecordBatches per dataset.
func New(storeLen int, logger *zap.Logger) *FlightServer {
	return &FlightServer{
		rawStore: newRecordStore(schema.AirQualitySchema, storeLen),
		aggStore: newRecordStore(aggregator.AggSchema, storeLen),
		logger:   logger,
	}
}

// AddRaw feeds a raw Measurement RecordBatch into the raw store.
func (s *FlightServer) AddRaw(rec arrow.Record) { s.rawStore.add(rec) }

// AddAgg feeds an aggregated RecordBatch into the agg store.
func (s *FlightServer) AddAgg(rec arrow.Record) { s.aggStore.add(rec) }

// Sizes returns (rawLen, aggLen) for diagnostics.
func (s *FlightServer) Sizes() (int, int) { return s.rawStore.len(), s.aggStore.len() }

// Start registers the service and begins listening on addr (e.g. "0.0.0.0:5005").
func (s *FlightServer) Start(addr string) error {
	s.srv = flight.NewServerWithMiddleware(nil)
	s.srv.RegisterFlightService(s)
	if err := s.srv.Init(addr); err != nil {
		return fmt.Errorf("flight init %s: %w", addr, err)
	}
	go func() {
		s.logger.Info("Arrow Flight server listening", zap.String("addr", addr))
		if err := s.srv.Serve(); err != nil {
			s.logger.Error("Flight server exited", zap.Error(err))
		}
	}()
	return nil
}

// Stop shuts the Flight gRPC server down gracefully.
func (s *FlightServer) Stop() {
	if s.srv != nil {
		s.srv.Shutdown()
	}
}

// ─── Flight RPC handlers ──────────────────────────────────────────────────────

// ListFlights advertises the "raw" and "agg" datasets.
func (s *FlightServer) ListFlights(_ *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	datasets := []struct {
		name  string
		store *recordStore
	}{{"raw", s.rawStore}, {"agg", s.aggStore}}

	for _, d := range datasets {
		ticket, _ := json.Marshal(TicketRequest{Dataset: d.name})
		info := &flight.FlightInfo{
			Schema: flight.SerializeSchema(d.store.schema, memory.DefaultAllocator),
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{d.name},
			},
			Endpoint:     []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
			TotalRecords: int64(d.store.len()),
			TotalBytes:   -1,
		}
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

// GetFlightInfo returns metadata for a single dataset by path descriptor.
func (s *FlightServer) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, fmt.Errorf("empty descriptor path")
	}
	st, err := s.storeFor(desc.Path[0])
	if err != nil {
		return nil, err
	}
	ticket, _ := json.Marshal(TicketRequest{Dataset: desc.Path[0]})
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(st.schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		TotalRecords:     int64(st.len()),
		TotalBytes:       -1,
	}, nil
}

// GetSchema returns the Arrow schema for a dataset without streaming any data.
func (s *FlightServer) GetSchema(_ context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if len(desc.Path) == 0 {
		return nil, fmt.Errorf("empty descriptor path")
	}
	st, err := s.storeFor(desc.Path[0])
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{
		Schema: flight.SerializeSchema(st.schema, memory.DefaultAllocator),
	}, nil
}

// DoGet streams all stored RecordBatches for the requested dataset.
// The ticket payload is a TicketRequest JSON.
func (s *FlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	var req TicketRequest
	if err := json.Unmarshal(ticket.Ticket, &req); err != nil {
		return fmt.Errorf("decode ticket: %w", err)
	}
	st, err := s.storeFor(req.Dataset)
	if err != nil {
		return err
	}

	records := st.snapshot()
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(st.schema))
	defer w.Close()

	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}
	s.logger.Info("DoGet served",
		zap.String("dataset", req.Dataset),
		zap.Int("batches", len(records)),
	)
	return nil
}

func (s *FlightServer) storeFor(name string) (*recordStore, error) {
	switch name {
	case "raw":
		return s.rawStore, nil
	case "agg":
		return s.aggStore, nil
	default:
		return nil, fmt.Errorf("unknown dataset %q (use \"raw\" or \"agg\")", name)
	}
}
