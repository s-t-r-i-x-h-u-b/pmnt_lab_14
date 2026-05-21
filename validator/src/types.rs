/// A single field-level validation failure.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field:   String,
    pub message: String,
}

/// Result of validating one air-quality measurement.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid:  bool,
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    pub fn ok() -> Self {
        Self { valid: true, errors: vec![] }
    }

    pub fn from_errors(errors: Vec<ValidationError>) -> Self {
        let valid = errors.is_empty();
        Self { valid, errors }
    }
}

/// Compact JSON serialisation for C callers (no serde dependency needed).
pub fn errors_to_json(errors: &[ValidationError]) -> String {
    let items: Vec<String> = errors
        .iter()
        .map(|e| {
            let f = e.field.replace('\\', "\\\\").replace('"', "\\\"");
            let m = e.message.replace('\\', "\\\\").replace('"', "\\\"");
            format!(r#"{{"field":"{f}","message":"{m}"}}"#)
        })
        .collect();
    format!("[{}]", items.join(","))
}
