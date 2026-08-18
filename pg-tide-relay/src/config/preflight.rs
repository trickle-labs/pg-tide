//! Shared, side-effect-free aggregate pipeline preflight.

use super::schema_support::{PipelineDocument, PIPELINE_SCHEMA_VERSION};
use super::PipelineConfig;
use crate::descriptors;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightIssue {
    pub pipeline: String,
    pub severity: PreflightSeverity,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreflightReport {
    pub issues: Vec<PreflightIssue>,
}

impl PreflightReport {
    pub fn is_valid(&self) -> bool {
        !self
            .issues
            .iter()
            .any(|issue| issue.severity == PreflightSeverity::Error)
    }
}

/// Validate every catalog row before ownership, polling, or worker creation.
/// Disabled rows are structurally validated too; unavailable disabled
/// connectors are warnings, while malformed rows remain errors.
pub fn validate_pipelines(pipelines: &[PipelineConfig]) -> PreflightReport {
    let mut issues = Vec::new();
    for pipeline in pipelines {
        match PipelineDocument::parse(&pipeline.name, &pipeline.config) {
            Ok(document) => {
                for (field, connector, descriptor) in [
                    (
                        "source_type",
                        document.source_type.as_str(),
                        descriptors::source_type_to_descriptor(&document.source_type),
                    ),
                    (
                        "sink_type",
                        document.sink_type.as_str(),
                        descriptors::sink_type_to_descriptor(&document.sink_type),
                    ),
                ] {
                    if !descriptor.is_some_and(descriptors::is_available) {
                        issues.push(PreflightIssue {
                            pipeline: pipeline.name.clone(),
                            severity: if pipeline.enabled {
                                PreflightSeverity::Error
                            } else {
                                PreflightSeverity::Warning
                            },
                            reason: format!("{field} '{connector}' is not compiled in"),
                        });
                    }
                }
                if let Some(descriptor) = descriptors::sink_type_to_descriptor(&document.sink_type)
                {
                    if !descriptors::is_supported_sink_type(&document.sink_type) {
                        issues.push(PreflightIssue {
                            pipeline: pipeline.name.clone(),
                            severity: PreflightSeverity::Warning,
                            reason: format!(
                                "sink_type '{}' is {} and is not a production-supported destination",
                                document.sink_type, descriptor.maturity
                            ),
                        });
                    }
                }
                if document.schema_version != PIPELINE_SCHEMA_VERSION {
                    issues.push(PreflightIssue {
                        pipeline: pipeline.name.clone(),
                        severity: PreflightSeverity::Error,
                        reason: "unsupported schema version".to_string(),
                    });
                }
            }
            Err(error) => issues.push(PreflightIssue {
                pipeline: pipeline.name.clone(),
                severity: PreflightSeverity::Error,
                reason: error.to_string(),
            }),
        }
    }
    issues.sort_by(|left, right| {
        (&left.pipeline, &left.reason).cmp(&(&right.pipeline, &right.reason))
    });
    PreflightReport { issues }
}

/// Alias for callers that use startup terminology.
pub fn startup_preflight(pipelines: &[PipelineConfig]) -> PreflightReport {
    validate_pipelines(pipelines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PipelineDirection;
    use serde_json::json;

    fn pipeline(name: &str, enabled: bool, source_type: &str) -> PipelineConfig {
        PipelineConfig {
            name: name.to_string(),
            direction: PipelineDirection::Forward,
            enabled,
            config: json!({
                "source_type": source_type,
                "source": {"outbox": "orders"},
                "sink_type": "stdout",
                "sink": {}
            }),
            tenant_name: "default".to_string(),
        }
    }

    #[test]
    fn report_order_is_deterministic_and_disabled_unavailable_is_warning() {
        let report = validate_pipelines(&[
            pipeline("z", false, "pg_logical"),
            pipeline("a", true, "unknown"),
        ]);
        assert_eq!(report.issues[0].pipeline, "a");
        assert_eq!(report.issues[1].pipeline, "z");
        assert!(matches!(
            report.issues[1].severity,
            PreflightSeverity::Warning
        ));
    }
}
