use super::output::Diagnostic;
use pg_tide_relay::error::{IncompatibleExtensionVersion, RelayError};

pub fn from_error(component: &str, _error: impl std::fmt::Display) -> Diagnostic {
    Diagnostic {
        code: "operator.failure".into(),
        component: component.into(),
        message: "The requested operation could not be completed.".into(),
        likely_cause: "The relay dependency or catalog state is unavailable.".into(),
        next_action: "Run `pg-tide doctor` and inspect the relay logs.".into(),
        installed_version: None,
        relay_version: None,
        policy_version: None,
        compatibility_class: None,
        supported_range: None,
    }
}

pub fn from_relay_error(component: &str, error: &RelayError) -> Diagnostic {
    match error {
        RelayError::IncompatibleExtensionVersion(error) => {
            let IncompatibleExtensionVersion {
                installed_version,
                relay_version,
                policy_version,
                compatibility_class,
                supported_range,
                next_action,
                ..
            } = error.as_ref();
            Diagnostic {
                code: "PGTIDE_EXTENSION_VERSION_INCOMPATIBLE".into(),
                component: component.into(),
                message: error.to_string(),
                likely_cause:
                    "The installed pg_tide extension is outside the relay lifecycle policy.".into(),
                next_action: next_action.clone(),
                installed_version: Some(installed_version.clone()),
                relay_version: Some(relay_version.clone()),
                policy_version: Some(policy_version.clone()),
                compatibility_class: Some(compatibility_class.clone()),
                supported_range: Some(supported_range.clone()),
            }
        }
        _ => from_error(component, error),
    }
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
        assert_eq!(value["installed_version"], "0.52.0");
        assert_eq!(value["relay_version"], "0.51.0");
        assert_eq!(value["policy_version"], "v1");
        assert_eq!(value["compatibility_class"], "incompatible");
        assert_eq!(value["supported_range"], "0.52.0..=0.53.0");
        assert!(value["next_action"].as_str().is_some());
    }
}
