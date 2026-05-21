//go:build rust_validator

package validator

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo linux  LDFLAGS: -L${SRCDIR} -lair_quality_validator -ldl -lm
#cgo darwin LDFLAGS: -L${SRCDIR} -lair_quality_validator -ldl -lm
#include "validator.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"

	"pmnt_lab14/collector/internal/schema"
)

func validateOne(m schema.Measurement) bool {
	cc := C.CString(m.CountryCode)
	param := C.CString(m.Parameter)
	defer C.free(unsafe.Pointer(cc))
	defer C.free(unsafe.Pointer(param))

	res := C.validate_measurement_c(
		cc, param,
		C.double(m.Value),
		C.double(m.Latitude),
		C.double(m.Longitude),
		C.int64_t(m.Timestamp.UnixMicro()),
	)
	defer C.free_validation_result(&res)
	return res.valid == 1
}

type cError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func filterImpl(measurements []schema.Measurement) ([]schema.Measurement, int) {
	valid := measurements[:0:0]
	invalid := 0
	for _, m := range measurements {
		if validateOne(m) {
			valid = append(valid, m)
		} else {
			invalid++
		}
	}
	return valid, invalid
}

// parseErrorsJSON is used only in tests / logging; not called on the hot path.
func parseErrorsJSON(jsonStr string) []Error {
	var raw []cError
	_ = json.Unmarshal([]byte(jsonStr), &raw)
	out := make([]Error, len(raw))
	for i, e := range raw {
		out[i] = Error{Field: e.Field, Message: e.Message}
	}
	return out
}
