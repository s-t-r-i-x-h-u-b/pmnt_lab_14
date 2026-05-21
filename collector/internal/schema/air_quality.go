package schema

import (
	"bytes"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

// Measurement is a single air quality reading from one sensor at one location.
type Measurement struct {
	LocationID   int64
	LocationName string
	CountryCode  string
	City         string
	Latitude     float64
	Longitude    float64
	Parameter    string
	Value        float64
	Unit         string
	Timestamp    time.Time
	CollectorID  string
}

var tsType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

var AirQualitySchema = arrow.NewSchema([]arrow.Field{
	{Name: "location_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	{Name: "location_name", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "country_code", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "latitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	{Name: "longitude", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	{Name: "parameter", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	{Name: "unit", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "timestamp", Type: tsType, Nullable: false},
	{Name: "collector_id", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

// EncodeToArrow encodes a batch of measurements into Arrow IPC stream format bytes.
func EncodeToArrow(measurements []Measurement) ([]byte, error) {
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, AirQualitySchema)
	defer b.Release()

	locIDs := b.Field(0).(*array.Int64Builder)
	locNames := b.Field(1).(*array.StringBuilder)
	countries := b.Field(2).(*array.StringBuilder)
	cities := b.Field(3).(*array.StringBuilder)
	lats := b.Field(4).(*array.Float64Builder)
	lons := b.Field(5).(*array.Float64Builder)
	params := b.Field(6).(*array.StringBuilder)
	vals := b.Field(7).(*array.Float64Builder)
	units := b.Field(8).(*array.StringBuilder)
	timestamps := b.Field(9).(*array.TimestampBuilder)
	collectorIDs := b.Field(10).(*array.StringBuilder)

	for _, m := range measurements {
		locIDs.Append(m.LocationID)
		locNames.Append(m.LocationName)
		countries.Append(m.CountryCode)
		if m.City == "" {
			cities.AppendNull()
		} else {
			cities.Append(m.City)
		}
		lats.Append(m.Latitude)
		lons.Append(m.Longitude)
		params.Append(m.Parameter)
		vals.Append(m.Value)
		units.Append(m.Unit)
		timestamps.Append(arrow.Timestamp(m.Timestamp.UnixMicro()))
		collectorIDs.Append(m.CollectorID)
	}

	rec := b.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(AirQualitySchema))
	if err := w.Write(rec); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
