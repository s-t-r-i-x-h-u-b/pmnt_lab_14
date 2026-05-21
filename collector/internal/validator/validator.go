// Package validator exposes a FilterMeasurements function backed either by the
// Rust air_quality_validator static library (when built with -tags rust_validator)
// or a pass-through stub (default, no cgo required).
package validator

import "pmnt_lab14/collector/internal/schema"

// Result is the validation outcome for one measurement.
type Result struct {
	Valid  bool
	Errors []Error
}

// Error is a single field-level validation failure.
type Error struct {
	Field   string
	Message string
}

// FilterMeasurements returns only the measurements that pass all validation
// rules. The second return value is the count of rejected measurements.
func FilterMeasurements(measurements []schema.Measurement) ([]schema.Measurement, int) {
	return filterImpl(measurements)
}
