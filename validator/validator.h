/* C header for the Rust air_quality_validator static library.
 * Used by Go via cgo: `#include "validator.h"`
 *
 * Build the static lib with:
 *   cargo build --release --no-default-features
 * Then copy target/release/libair_quality_validator.a next to this header.
 */
#ifndef AIR_QUALITY_VALIDATOR_H
#define AIR_QUALITY_VALIDATOR_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Result of validating one measurement.
 * `errors_json` is a heap-allocated NUL-terminated JSON string of the form:
 *   [{"field":"...","message":"..."},...]
 * It is NULL when valid == 1.
 * Caller MUST call free_validation_result() to release the memory.
 */
typedef struct {
    int    valid;        /* 1 = valid, 0 = invalid */
    char*  errors_json;  /* NULL when valid */
} CValidationResult;

/**
 * Validate a single air-quality measurement.
 * All char* arguments must be valid, NUL-terminated C strings.
 */
CValidationResult validate_measurement_c(
    const char* country_code,
    const char* parameter,
    double      value,
    double      latitude,
    double      longitude,
    int64_t     timestamp_us   /* microseconds since Unix epoch */
);

/**
 * Free the errors_json string inside the result.
 * Safe to call with NULL.
 */
void free_validation_result(CValidationResult* result);

#ifdef __cplusplus
}
#endif

#endif /* AIR_QUALITY_VALIDATOR_H */
