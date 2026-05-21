mod rules;
mod types;

pub use types::{ValidationError, ValidationResult};
use rules::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// ── Core validation (pure Rust, no FFI) ──────────────────────────────────────

/// Validate a single air-quality measurement.
pub fn validate_measurement(
    country_code: &str,
    parameter:    &str,
    value:        f64,
    latitude:     f64,
    longitude:    f64,
    timestamp_us: i64,
) -> ValidationResult {
    let mut errors = vec![];

    if let Some(e) = validate_country_code(country_code) { errors.push(e); }
    if let Some(e) = validate_parameter(parameter)       { errors.push(e); }
    if let Some(e) = validate_value(parameter, value)    { errors.push(e); }
    errors.extend(validate_coordinates(latitude, longitude));
    if let Some(e) = validate_timestamp(timestamp_us)    { errors.push(e); }

    ValidationResult::from_errors(errors)
}

/// Validate a batch; returns one ValidationResult per record (same order).
pub fn validate_batch(
    country_codes: &[&str],
    parameters:    &[&str],
    values:        &[f64],
    latitudes:     &[f64],
    longitudes:    &[f64],
    timestamps_us: &[i64],
) -> Vec<ValidationResult> {
    let n = country_codes.len();
    (0..n)
        .map(|i| validate_measurement(
            country_codes[i], parameters[i], values[i],
            latitudes[i],     longitudes[i], timestamps_us[i],
        ))
        .collect()
}

// ── C FFI – used by Go via cgo ────────────────────────────────────────────────

/// Result returned to C callers.  `errors_json` is a heap-allocated C string
/// (JSON array of {field,message} objects) and MUST be freed with
/// `free_validation_result`.  It is NULL when `valid == 1`.
#[repr(C)]
pub struct CValidationResult {
    pub valid:       i32,
    pub errors_json: *mut c_char,
}

/// Validate a single measurement.  All string pointers must be valid,
/// non-null, NUL-terminated C strings.
///
/// # Safety
/// Caller must ensure all pointer arguments are valid.
#[no_mangle]
pub unsafe extern "C" fn validate_measurement_c(
    country_code: *const c_char,
    parameter:    *const c_char,
    value:        f64,
    latitude:     f64,
    longitude:    f64,
    timestamp_us: i64,
) -> CValidationResult {
    let cc    = unsafe { CStr::from_ptr(country_code) }.to_str().unwrap_or("");
    let param = unsafe { CStr::from_ptr(parameter)    }.to_str().unwrap_or("");

    let result = validate_measurement(cc, param, value, latitude, longitude, timestamp_us);

    if result.valid {
        CValidationResult { valid: 1, errors_json: std::ptr::null_mut() }
    } else {
        let json = types::errors_to_json(&result.errors);
        let cstr = CString::new(json).unwrap_or_default();
        CValidationResult { valid: 0, errors_json: cstr.into_raw() }
    }
}

/// Free the `errors_json` string returned by `validate_measurement_c`.
/// Safe to call with a NULL pointer.
///
/// # Safety
/// `result` must point to a `CValidationResult` returned by `validate_measurement_c`.
#[no_mangle]
pub unsafe extern "C" fn free_validation_result(result: *mut CValidationResult) {
    if result.is_null() { return; }
    let r = unsafe { &mut *result };
    if !r.errors_json.is_null() {
        drop(unsafe { CString::from_raw(r.errors_json) });
        r.errors_json = std::ptr::null_mut();
    }
}

// ── PyO3 module – used by Python ──────────────────────────────────────────────

#[cfg(feature = "python")]
mod python_ext {
    use super::*;
    use pyo3::prelude::*;

    /// Per-field validation error exposed to Python.
    #[pyclass(frozen)]
    #[derive(Clone)]
    pub struct PyValidationError {
        #[pyo3(get)] pub field:   String,
        #[pyo3(get)] pub message: String,
    }

    #[pymethods]
    impl PyValidationError {
        fn __repr__(&self) -> String {
            format!("ValidationError(field={:?}, message={:?})", self.field, self.message)
        }
    }

    /// Validation result for one measurement.
    #[pyclass(frozen)]
    pub struct PyValidationResult {
        #[pyo3(get)] pub valid:  bool,
        #[pyo3(get)] pub errors: Vec<PyValidationError>,
    }

    #[pymethods]
    impl PyValidationResult {
        fn __repr__(&self) -> String {
            format!("ValidationResult(valid={}, errors={})", self.valid, self.errors.len())
        }
    }

    fn to_py_result(r: ValidationResult) -> PyValidationResult {
        PyValidationResult {
            valid:  r.valid,
            errors: r.errors.into_iter().map(|e| PyValidationError {
                field:   e.field,
                message: e.message,
            }).collect(),
        }
    }

    /// Validate a single measurement.
    #[pyfunction]
    #[pyo3(signature = (country_code, parameter, value, latitude, longitude, timestamp_us))]
    fn validate_measurement_py(
        country_code: &str,
        parameter:    &str,
        value:        f64,
        latitude:     f64,
        longitude:    f64,
        timestamp_us: i64,
    ) -> PyValidationResult {
        to_py_result(super::validate_measurement(
            country_code, parameter, value, latitude, longitude, timestamp_us,
        ))
    }

    /// Validate a batch of measurements.  All lists must have the same length.
    #[pyfunction]
    fn validate_batch_py(
        country_codes: Vec<String>,
        parameters:    Vec<String>,
        values:        Vec<f64>,
        latitudes:     Vec<f64>,
        longitudes:    Vec<f64>,
        timestamps_us: Vec<i64>,
    ) -> Vec<PyValidationResult> {
        let cc:  Vec<&str> = country_codes.iter().map(|s| s.as_str()).collect();
        let par: Vec<&str> = parameters.iter().map(|s| s.as_str()).collect();
        super::validate_batch(&cc, &par, &values, &latitudes, &longitudes, &timestamps_us)
            .into_iter()
            .map(to_py_result)
            .collect()
    }

    /// Convenience: validate every row of a dict-of-lists (column-oriented).
    /// Returns a tuple (valid_flags, error_counts) for easy pandas integration.
    #[pyfunction]
    fn validate_columns(
        py:            Python<'_>,
        country_codes: Vec<String>,
        parameters:    Vec<String>,
        values:        Vec<f64>,
        latitudes:     Vec<f64>,
        longitudes:    Vec<f64>,
        timestamps_us: Vec<i64>,
    ) -> PyResult<PyObject> {
        let results = validate_batch_py(
            country_codes, parameters, values, latitudes, longitudes, timestamps_us,
        );
        let valid_flags: Vec<bool>   = results.iter().map(|r| r.valid).collect();
        let err_counts:  Vec<usize>  = results.iter().map(|r| r.errors.len()).collect();
        Ok((valid_flags, err_counts).into_py(py))
    }

    #[pymodule]
    fn air_quality_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PyValidationError>()?;
        m.add_class::<PyValidationResult>()?;
        m.add_function(wrap_pyfunction!(validate_measurement_py, m)?)?;
        m.add_function(wrap_pyfunction!(validate_batch_py,       m)?)?;
        m.add_function(wrap_pyfunction!(validate_columns,        m)?)?;
        Ok(())
    }
}
