//go:build !rust_validator

package validator

import "pmnt_lab14/collector/internal/schema"

// filterImpl is the pass-through stub used when the Rust library is not linked.
// All measurements are considered valid.
func filterImpl(measurements []schema.Measurement) ([]schema.Measurement, int) {
	return measurements, 0
}
