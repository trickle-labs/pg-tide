use crate::error::{IncompatibleExtensionVersion, RelayError};
use serde::{Deserialize, Serialize};

pub const COMPATIBILITY_ERROR_CODE: &str = "PGTIDE_EXTENSION_VERSION_INCOMPATIBLE";
const EMBEDDED_POLICY: &str = include_str!("../schemas/lifecycle-compatibility-v1.json");

#[derive(Debug, Deserialize)]
struct EmbeddedPolicy {
    policy_version: String,
    compatibility_error_code: String,
    relay_window: RelayWindow,
    matrix: Vec<MatrixRow>,
}

#[derive(Debug, Deserialize)]
struct RelayWindow {
    minimum: String,
    maximum: String,
}

#[derive(Debug, Deserialize)]
struct MatrixRow {
    extension: String,
    relay: String,
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRow {
    pub extension_version: String,
    pub relay_version: String,
    pub compatibility_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompatibilityDecision {
    pub relay_version: String,
    pub extension_version: String,
    pub policy_version: String,
    pub compatibility_class: String,
    pub supported_range: String,
    pub next_action: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Version(u32, u32, u32);

fn parse_release(value: &str) -> Option<Version> {
    let mut parts = value.split('.');
    let version = Version(
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    if parts.next().is_some()
        || value.split('.').any(|part| part.is_empty())
        || value
            .split('.')
            .any(|part| part.len() > 1 && part.starts_with('0'))
    {
        return None;
    }
    Some(version)
}

fn embedded_policy() -> Result<EmbeddedPolicy, Box<RelayError>> {
    let policy: EmbeddedPolicy = serde_json::from_str(EMBEDDED_POLICY).map_err(|error| {
        RelayError::config(format!("embedded lifecycle policy is invalid: {error}"))
    })?;
    if policy.compatibility_error_code != COMPATIBILITY_ERROR_CODE {
        return Err(Box::new(RelayError::config(
            "embedded lifecycle policy has an unexpected compatibility error code",
        )));
    }
    Ok(policy)
}

fn window_bounds(policy: &EmbeddedPolicy) -> Result<(Version, Version), Box<RelayError>> {
    let minimum = parse_release(&policy.relay_window.minimum).ok_or_else(|| {
        RelayError::config("embedded lifecycle policy has a malformed minimum version")
    })?;
    let maximum = parse_release(&policy.relay_window.maximum).ok_or_else(|| {
        RelayError::config("embedded lifecycle policy has a malformed maximum version")
    })?;
    Ok((minimum, maximum))
}

fn bounded_version(value: Option<&str>) -> String {
    value
        .unwrap_or("<missing>")
        .chars()
        .take(64)
        .map(|character| {
            if character.is_control() {
                '?'
            } else {
                character
            }
        })
        .collect()
}

fn incompatible(
    relay_version: &str,
    extension_version: Option<&str>,
    reason: &str,
    policy: &EmbeddedPolicy,
) -> Box<RelayError> {
    Box::new(RelayError::IncompatibleExtensionVersion(Box::new(
        IncompatibleExtensionVersion {
            installed_version: bounded_version(extension_version),
            relay_version: bounded_version(Some(relay_version)),
            policy_version: policy.policy_version.clone(),
            compatibility_class: "incompatible".to_string(),
            supported_range: format!(
                "{}..={}",
                policy.relay_window.minimum, policy.relay_window.maximum
            ),
            next_action: format!(
                "Upgrade or restore pg_tide to {} or {}, then restart pg-tide.",
                policy.relay_window.minimum, policy.relay_window.maximum
            ),
            reason: reason.to_string(),
        },
    )))
}

pub fn evaluate(
    relay_version: &str,
    extension_version: Option<&str>,
) -> Result<CompatibilityDecision, Box<RelayError>> {
    let policy = embedded_policy()?;
    let (minimum, maximum) = window_bounds(&policy)?;
    let relay = parse_release(relay_version).ok_or_else(|| {
        incompatible(
            relay_version,
            extension_version,
            "malformed relay version",
            &policy,
        )
    })?;
    let extension_value = extension_version.ok_or_else(|| {
        incompatible(
            relay_version,
            None,
            "pg_tide is missing from pg_extension",
            &policy,
        )
    })?;
    let extension = parse_release(extension_value).ok_or_else(|| {
        incompatible(
            relay_version,
            Some(extension_value),
            "malformed extension version",
            &policy,
        )
    })?;

    let row = policy
        .matrix
        .iter()
        .filter(|row| row.status == "supported")
        .find(|row| {
            parse_release(&row.relay) == Some(relay)
                && parse_release(&row.extension) == Some(extension)
        });
    match row {
        Some(row) => Ok(CompatibilityDecision {
            relay_version: row.relay.clone(),
            extension_version: row.extension.clone(),
            policy_version: policy.policy_version.clone(),
            compatibility_class: row.status.clone(),
            supported_range: format!(
                "{}..={}",
                policy.relay_window.minimum, policy.relay_window.maximum
            ),
            next_action: format!(
                "Upgrade or restore pg_tide to {} or {}, then restart pg-tide.",
                policy.relay_window.minimum, policy.relay_window.maximum
            ),
        }),
        None => Err(incompatible(
            relay_version,
            Some(extension_value),
            if extension < minimum {
                "extension version is below the supported runtime window"
            } else if extension > maximum {
                "extension version is newer than the supported runtime window"
            } else {
                "relay and extension versions are not listed in the policy"
            },
            &policy,
        )),
    }
}

pub fn policy_rows() -> Result<Vec<PolicyRow>, Box<RelayError>> {
    Ok(embedded_policy()?
        .matrix
        .into_iter()
        .filter(|row| {
            row.status == "supported"
                && parse_release(&row.extension).is_some()
                && parse_release(&row.relay).is_some()
        })
        .map(|row| PolicyRow {
            extension_version: row.extension,
            relay_version: row.relay,
            compatibility_class: row.status,
        })
        .collect())
}

pub async fn check_client(
    client: &tokio_postgres::Client,
    relay_version: &str,
) -> Result<CompatibilityDecision, Box<RelayError>> {
    let extension_version = client
        .query_opt(
            "SELECT extversion::text FROM pg_extension WHERE extname = 'pg_tide'",
            &[],
        )
        .await
        .map_err(|error| Box::new(RelayError::Postgres(error)))?
        .map(|row| row.get::<_, String>(0));
    evaluate(relay_version, extension_version.as_deref())
}

pub async fn check_url(
    url: &str,
    relay_version: &str,
) -> Result<CompatibilityDecision, Box<RelayError>> {
    let (client, connection) = crate::pg_tls::connect(url).await.map_err(Box::new)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    check_client(&client, relay_version).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_every_supported_matrix_row() {
        for row in policy_rows().unwrap() {
            let decision = evaluate(&row.relay_version, Some(&row.extension_version)).unwrap();
            assert_eq!(decision.compatibility_class, "supported");
        }
    }

    #[test]
    fn rejects_lower_and_upper_bounds() {
        assert!(evaluate("0.52.0", Some("0.50.0")).is_err());
        assert!(evaluate("0.52.0", Some("0.53.0")).is_err());
    }

    #[test]
    fn rejects_malformed_prerelease_future_and_missing_versions() {
        for extension in [Some("0.51"), Some("0.51.0-rc.1"), Some("0.52.0")] {
            assert!(evaluate("0.51.0", extension).is_err());
        }
        assert!(evaluate("0.51.0", None).is_err());
    }

    #[test]
    fn error_text_is_bounded_and_deterministic() {
        let error = evaluate("0.51.0", Some("not-a-version")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "PGTIDE_EXTENSION_VERSION_INCOMPATIBLE: installed_version=not-a-version; relay_version=0.51.0; policy_version=v1; compatibility_class=incompatible; supported_range=0.51.0..=0.52.0; next_action=Upgrade or restore pg_tide to 0.51.0 or 0.52.0, then restart pg-tide.; reason=malformed extension version"
        );
    }
}
