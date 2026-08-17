use crate::cli::OutputFormat;
use chrono::Utc;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Envelope<T: Serialize> {
    pub schema_version: u32,
    pub command: String,
    pub ok: bool,
    pub observed_at: String,
    pub data: Option<T>,
    pub error: Option<Diagnostic>,
}

#[derive(Debug, Serialize)]
pub struct Diagnostic {
    pub code: String,
    pub component: String,
    pub message: String,
    pub likely_cause: String,
    pub next_action: String,
}

pub fn success<T: Serialize>(
    command: &str,
    data: T,
    format: OutputFormat,
) -> Result<(), serde_json::Error> {
    if matches!(format, OutputFormat::Json) {
        let json = serde_json::to_string(&Envelope {
            schema_version: 1,
            command: command.to_owned(),
            ok: true,
            observed_at: Utc::now().to_rfc3339(),
            data: Some(data),
            error: None,
        })?;
        println!("{json}");
    }
    Ok(())
}

pub fn failure(
    command: &str,
    diagnostic: Diagnostic,
    format: OutputFormat,
) -> Result<(), serde_json::Error> {
    if matches!(format, OutputFormat::Json) {
        let json = serde_json::to_string(&Envelope::<serde_json::Value> {
            schema_version: 1,
            command: command.to_owned(),
            ok: false,
            observed_at: Utc::now().to_rfc3339(),
            data: None,
            error: Some(diagnostic),
        })?;
        println!("{json}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_schema_v1_envelope() {
        let value = serde_json::to_value(Envelope {
            schema_version: 1,
            command: "doctor".into(),
            ok: true,
            observed_at: "2026-01-01T00:00:00Z".into(),
            data: Some(vec!["ok"]),
            error: None,
        })
        .unwrap();
        assert_eq!(value["schema_version"], 1);
        assert!(value["error"].is_null());
    }
}
