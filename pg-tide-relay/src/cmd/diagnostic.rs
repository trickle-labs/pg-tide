use super::output::Diagnostic;
use crate::operator_errors;
use pg_tide_relay::error::RelayError;

pub fn from_error(component: &str, error: impl std::fmt::Display) -> Diagnostic {
    // ponytail: classify legacy boxed command errors from their bounded wording;
    // replace with typed RelayError returns when those command APIs are revised.
    let text = error.to_string().to_ascii_lowercase();
    let code = if text.contains("support.bundle") {
        "PGTIDE_SUPPORT_BUNDLE_WRITE_FAILED"
    } else if text.contains("shutdown") {
        "PGTIDE_SHUTDOWN_FAILED"
    } else if text.contains("replay") || text.contains("dlq") {
        "PGTIDE_REPLAY_INPUT_INVALID"
    } else if text.contains("webhook") && text.contains("ssrf") {
        "PGTIDE_WEBHOOK_SSRF_REJECTED"
    } else if text.contains("tls") || text.contains("ssl") {
        "PGTIDE_TLS_VERIFICATION_FAILED"
    } else if text.contains("authentication") {
        "PGTIDE_POSTGRES_AUTHENTICATION"
    } else if text.contains("not found") && text.contains("pipeline") {
        "PGTIDE_PIPELINE_NOT_FOUND"
    } else if text.contains("not discovered") {
        "PGTIDE_PIPELINE_NOT_DISCOVERED"
    } else if text.contains("not owned") {
        "PGTIDE_PIPELINE_NOT_OWNED"
    } else if text.contains("catalog") && text.contains("missing") {
        "PGTIDE_CATALOG_MISSING"
    } else if text.contains("preflight") || text.contains("invalid config") {
        "PGTIDE_PIPELINE_INVALID"
    } else if text.contains("permission") || text.contains("authorization") {
        "PGTIDE_POSTGRES_AUTHORIZATION"
    } else if text.contains("connection failed") || text.contains("connect") {
        "PGTIDE_POSTGRES_UNAVAILABLE"
    } else if text.contains("sweep") {
        "PGTIDE_MAINTENANCE_SWEEP_FAILED"
    } else {
        "operator.failure"
    };
    diagnostic(component, code)
}

fn diagnostic(component: &str, code: &str) -> Diagnostic {
    let descriptor =
        match operator_errors::find(code).or_else(|| operator_errors::find("operator.failure")) {
            Some(descriptor) => descriptor,
            None => {
                return Diagnostic {
                    code: "operator.failure".into(),
                    component: component.into(),
                    message: "The requested operation could not be completed.".into(),
                    likely_cause: "The relay dependency or catalog state is unavailable.".into(),
                    next_action: "Run `pg-tide doctor` and inspect the relay logs.".into(),
                };
            }
        };
    let next_action = if code == "operator.failure" {
        descriptor.next_action.to_string()
    } else {
        format!(
            "{} (runbook: {}).",
            descriptor.next_action, descriptor.runbook
        )
    };
    Diagnostic {
        code: descriptor.code.to_string(),
        component: component.into(),
        message: descriptor.summary.into(),
        likely_cause: descriptor.likely_cause.into(),
        next_action,
    }
}

pub fn from_relay_error(component: &str, error: &RelayError) -> Diagnostic {
    diagnostic(component, error.operator_code_for(component))
}

pub fn from_boxed_error(component: &str, error: &(dyn std::error::Error + 'static)) -> Diagnostic {
    error.downcast_ref::<RelayError>().map_or_else(
        || from_error(component, error),
        |error| from_relay_error(component, error),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_json_has_stable_public_fields() {
        let error = pg_tide_relay::compatibility::evaluate("0.51.0", Some("0.52.0"))
            .expect_err("future extension must be rejected");
        let value =
            serde_json::to_value(from_relay_error("postgres.extension", error.as_ref())).unwrap();
        assert_eq!(value["code"], "PGTIDE_EXTENSION_VERSION_INCOMPATIBLE");
        assert_eq!(value.as_object().unwrap().len(), 5);
        assert!(value["next_action"]
            .as_str()
            .unwrap()
            .contains("failed-upgrade"));
    }

    #[test]
    fn connector_failure_uses_catalog_code_and_frozen_fields() {
        let error = RelayError::connector_failure(
            "nats",
            pg_tide_relay::error::ConnectorFailureCode::Authentication,
            pg_tide_relay::error::RetryClass::Permanent,
            "secret text must not be copied",
        );
        let value = serde_json::to_value(from_relay_error("connector.nats", &error)).unwrap();
        assert_eq!(value["code"], "PGTIDE_CONNECTOR_AUTHENTICATION");
        assert_eq!(value.as_object().unwrap().len(), 5);
        assert!(!value.to_string().contains("secret text"));
    }
}
