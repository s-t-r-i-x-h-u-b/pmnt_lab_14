package aggregator

import (
	"bytes"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

var aggTSType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

// AggSchema is the Apache Arrow schema for aggregated window records.
var AggSchema = arrow.NewSchema([]arrow.Field{
	{Name: "window_start",   Type: aggTSType},
	{Name: "window_end",     Type: aggTSType},
	{Name: "country_code",   Type: arrow.BinaryTypes.String},
	{Name: "parameter",      Type: arrow.BinaryTypes.String},
	{Name: "unit",           Type: arrow.BinaryTypes.String},
	{Name: "count",          Type: arrow.PrimitiveTypes.Int64},
	{Name: "mean_value",     Type: arrow.PrimitiveTypes.Float64},
	{Name: "min_value",      Type: arrow.PrimitiveTypes.Float64},
	{Name: "max_value",      Type: arrow.PrimitiveTypes.Float64},
	{Name: "std_value",      Type: arrow.PrimitiveTypes.Float64},
	{Name: "location_count", Type: arrow.PrimitiveTypes.Int64},
	{Name: "collector_id",   Type: arrow.BinaryTypes.String},
}, nil)

// BuildRecord creates an Arrow Record for AggRecords.
// The caller owns the record and must call rec.Release() when done.
func BuildRecord(records []AggRecord) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, AggSchema)
	defer b.Release()

	winStarts := b.Field(0).(*array.TimestampBuilder)
	winEnds   := b.Field(1).(*array.TimestampBuilder)
	countries := b.Field(2).(*array.StringBuilder)
	params    := b.Field(3).(*array.StringBuilder)
	units     := b.Field(4).(*array.StringBuilder)
	counts    := b.Field(5).(*array.Int64Builder)
	means     := b.Field(6).(*array.Float64Builder)
	mins      := b.Field(7).(*array.Float64Builder)
	maxs      := b.Field(8).(*array.Float64Builder)
	stds      := b.Field(9).(*array.Float64Builder)
	locCounts := b.Field(10).(*array.Int64Builder)
	collIDs   := b.Field(11).(*array.StringBuilder)

	for _, r := range records {
		winStarts.Append(arrow.Timestamp(r.WindowStart.UnixMicro()))
		winEnds.Append(arrow.Timestamp(r.WindowEnd.UnixMicro()))
		countries.Append(r.CountryCode)
		params.Append(r.Parameter)
		units.Append(r.Unit)
		counts.Append(r.Count)
		means.Append(r.MeanValue)
		mins.Append(r.MinValue)
		maxs.Append(r.MaxValue)
		stds.Append(r.StdValue)
		locCounts.Append(r.LocationCount)
		collIDs.Append(r.CollectorID)
	}
	return b.NewRecord(), nil
}

// EncodeToArrow serialises AggRecords to Arrow IPC stream bytes (for NATS).
func EncodeToArrow(records []AggRecord) ([]byte, error) {
	rec, err := BuildRecord(records)
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(AggSchema))
	if err := w.Write(rec); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
