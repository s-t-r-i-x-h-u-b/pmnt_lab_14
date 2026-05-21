use crate::types::ValidationError;
use chrono::Utc;

// ── Country code ─────────────────────────────────────────────────────────────

pub fn validate_country_code(code: &str) -> Option<ValidationError> {
    if code.len() == 2 && code.chars().all(|c| c.is_ascii_uppercase()) {
        None
    } else {
        Some(ValidationError {
            field:   "country_code".into(),
            message: format!("must be 2 uppercase ASCII letters (ISO 3166-1), got {:?}", code),
        })
    }
}

// ── Parameter name ────────────────────────────────────────────────────────────

const KNOWN_PARAMS: &[&str] = &[
    "pm25", "pm10", "o3", "no2", "so2", "co", "bc",
    "humidity", "temperature", "pressure", "um025", "um010",
];

pub fn validate_parameter(param: &str) -> Option<ValidationError> {
    if param.is_empty() {
        return Some(ValidationError {
            field:   "parameter".into(),
            message: "must not be empty".into(),
        });
    }
    let lower = param.to_lowercase();
    if !KNOWN_PARAMS.contains(&lower.as_str()) {
        // Unknown parameters arrive from OpenAQ regularly; treat as a warning, not error.
        // Return None so the record is still accepted.
    }
    None
}

// ── Value range ───────────────────────────────────────────────────────────────

pub fn validate_value(parameter: &str, value: f64) -> Option<ValidationError> {
    if !value.is_finite() {
        return Some(ValidationError {
            field:   "value".into(),
            message: format!("must be a finite number, got {value}"),
        });
    }
    let (min, max) = param_range(parameter);
    if value < min || value > max {
        Some(ValidationError {
            field:   "value".into(),
            message: format!(
                "{value} outside expected range [{min}, {max}] for parameter {parameter:?}",
            ),
        })
    } else {
        None
    }
}

fn param_range(param: &str) -> (f64, f64) {
    match param.to_lowercase().as_str() {
        "pm25"        => (0.0,     2_000.0),
        "pm10"        => (0.0,     3_000.0),
        "o3"          => (0.0,       600.0),
        "no2"         => (0.0,     2_000.0),
        "so2"         => (0.0,     2_000.0),
        "co"          => (0.0,   100_000.0),
        "bc"          => (0.0,       200.0),
        "humidity"    => (0.0,       100.0),
        "temperature" => (-100.0,    100.0),
        "pressure"    => (80_000.0, 110_000.0),
        _             => (-1e9,      1e9),    // wide range for unknown params
    }
}

// ── Coordinates ───────────────────────────────────────────────────────────────

pub fn validate_coordinates(lat: f64, lon: f64) -> Vec<ValidationError> {
    let mut errs = vec![];
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        errs.push(ValidationError {
            field:   "latitude".into(),
            message: format!("must be in [-90, 90], got {lat}"),
        });
    }
    if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
        errs.push(ValidationError {
            field:   "longitude".into(),
            message: format!("must be in [-180, 180], got {lon}"),
        });
    }
    errs
}

// ── Timestamp ─────────────────────────────────────────────────────────────────

const MAX_FUTURE_US: i64  = 5 * 60 * 1_000_000;          // 5 minutes
const MAX_AGE_US:    i64  = 30 * 24 * 3600 * 1_000_000;   // 30 days

pub fn validate_timestamp(ts_micros: i64) -> Option<ValidationError> {
    let now = Utc::now().timestamp_micros();
    if ts_micros > now + MAX_FUTURE_US {
        let secs = (ts_micros - now) / 1_000_000;
        Some(ValidationError {
            field:   "timestamp".into(),
            message: format!("timestamp is {secs}s in the future"),
        })
    } else if ts_micros < now - MAX_AGE_US {
        let days = (now - ts_micros) / (24 * 3600 * 1_000_000);
        Some(ValidationError {
            field:   "timestamp".into(),
            message: format!("timestamp is {days} days in the past (limit: 30 days)"),
        })
    } else {
        None
    }
}
