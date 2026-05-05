/// JMESPath message transforms (RELAY-P2-13).
///
/// Applies lightweight payload transformations before publishing to a sink
/// (forward) or writing to the inbox (reverse).  Two operations are supported:
///
/// - **filter**: A JMESPath expression evaluated against the message payload.
///   If the result is `null` or `false`, the message is dropped.
/// - **transform**: A JMESPath expression whose result replaces the payload.
///
/// Configuration lives in the pipeline's `config` JSONB column:
///
/// ```json
/// { "transform": { "filter": "status", "payload": "{ id: id, total: amount }" } }
/// ```
use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// Transform configuration parsed from a pipeline's JSON config.
#[derive(Debug, Clone, Default)]
pub struct TransformConfig {
    /// JMESPath filter expression. Message is kept if result is truthy.
    pub filter: Option<String>,
    /// JMESPath projection expression. Replaces the payload with the result.
    pub payload_expr: Option<String>,
}

impl TransformConfig {
    /// Parse transform config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let t = match config.get("transform") {
            Some(t) => t,
            None => return Self::default(),
        };

        Self {
            filter: t.get("filter").and_then(|v| v.as_str()).map(String::from),
            payload_expr: t.get("payload").and_then(|v| v.as_str()).map(String::from),
        }
    }

    /// Returns true if no transforms are configured.
    pub fn is_identity(&self) -> bool {
        self.filter.is_none() && self.payload_expr.is_none()
    }
}

/// Apply transforms to a batch of messages.
///
/// Returns the filtered and/or transformed messages.
/// Messages dropped by the filter are silently removed from the result.
pub fn apply_transforms(
    config: &TransformConfig,
    messages: Vec<RelayMessage>,
) -> Result<Vec<RelayMessage>, RelayError> {
    if config.is_identity() {
        return Ok(messages);
    }

    let mut result = Vec::with_capacity(messages.len());

    for msg in messages {
        if let Some(transformed) = apply_one(config, msg)? {
            result.push(transformed);
        }
    }

    Ok(result)
}

/// Apply transforms to a single message.  Returns None if filtered out.
pub fn apply_one(
    config: &TransformConfig,
    mut msg: RelayMessage,
) -> Result<Option<RelayMessage>, RelayError> {
    // Apply filter first.
    if let Some(ref filter_expr) = config.filter {
        let passed = eval_filter(filter_expr, &msg.payload)
            .map_err(|e| RelayError::other(format!("transform filter error: {e}")))?;
        if !passed {
            return Ok(None);
        }
    }

    // Apply payload projection.
    if let Some(ref payload_expr) = config.payload_expr {
        let new_payload = eval_projection(payload_expr, &msg.payload)
            .map_err(|e| RelayError::other(format!("transform payload error: {e}")))?;
        msg.payload = new_payload;
    }

    Ok(Some(msg))
}

/// Evaluate a JMESPath expression as a filter predicate.
/// Returns true if the result is a non-null, non-false value.
fn eval_filter(expr: &str, payload: &serde_json::Value) -> Result<bool, String> {
    let compiled = jmespath::compile(expr)
        .map_err(|e| format!("invalid JMESPath expression '{expr}': {e}"))?;

    let json_str =
        serde_json::to_string(payload).map_err(|e| format!("payload serialization error: {e}"))?;
    let result = compiled
        .search(json_str.as_str())
        .map_err(|e| format!("JMESPath evaluation error: {e}"))?;

    Ok(is_truthy(&result))
}

/// Evaluate a JMESPath expression as a payload projection.
/// Returns the resulting JSON value.
fn eval_projection(expr: &str, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
    let compiled = jmespath::compile(expr)
        .map_err(|e| format!("invalid JMESPath expression '{expr}': {e}"))?;

    let json_str =
        serde_json::to_string(payload).map_err(|e| format!("payload serialization error: {e}"))?;
    let result = compiled
        .search(json_str.as_str())
        .map_err(|e| format!("JMESPath evaluation error: {e}"))?;

    variable_to_json(&result).map_err(|e| format!("JMESPath result conversion error: {e}"))
}

/// Convert a JMESPath `Variable` result to a `serde_json::Value`.
fn variable_to_json(var: &jmespath::Variable) -> Result<serde_json::Value, serde_json::Error> {
    let s = serde_json::to_string(var)?;
    serde_json::from_str(&s)
}

/// JMESPath truthiness: null and false are falsy; everything else is truthy.
fn is_truthy(var: &jmespath::Variable) -> bool {
    match var {
        jmespath::Variable::Null => false,
        jmespath::Variable::Bool(b) => *b,
        jmespath::Variable::String(s) => !s.is_empty(),
        jmespath::Variable::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        jmespath::Variable::Array(a) => !a.is_empty(),
        jmespath::Variable::Object(o) => !o.is_empty(),
        jmespath::Variable::Expref(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn make_msg(payload: serde_json::Value) -> RelayMessage {
        RelayMessage::new_reverse("key-1", "event", payload)
    }

    #[test]
    fn test_identity_transform_passthrough() {
        let config = TransformConfig::default();
        let msgs = vec![make_msg(serde_json::json!({"id": 1}))];
        let result = apply_transforms(&config, msgs).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_filter_keeps_matching() {
        let config = TransformConfig {
            filter: Some("status".to_string()),
            payload_expr: None,
        };
        let msg = make_msg(serde_json::json!({"status": "active", "id": 1}));
        let result = apply_one(&config, msg).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_filter_drops_non_matching() {
        let config = TransformConfig {
            filter: Some("status".to_string()),
            payload_expr: None,
        };
        let msg = make_msg(serde_json::json!({"id": 1})); // no "status" field → null → falsy
        let result = apply_one(&config, msg).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter_drops_false_value() {
        let config = TransformConfig {
            filter: Some("enabled".to_string()),
            payload_expr: None,
        };
        let msg = make_msg(serde_json::json!({"enabled": false}));
        let result = apply_one(&config, msg).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_payload_projection() {
        let config = TransformConfig {
            filter: None,
            payload_expr: Some("id".to_string()),
        };
        let msg = make_msg(serde_json::json!({"id": 42, "secret": "hidden"}));
        let result = apply_one(&config, msg).unwrap().unwrap();
        assert_eq!(result.payload, serde_json::json!(42));
    }

    #[test]
    fn test_filter_and_project_combined() {
        let config = TransformConfig {
            filter: Some("active".to_string()),
            payload_expr: Some("id".to_string()),
        };

        // Passes filter, projection applied
        let msg = make_msg(serde_json::json!({"id": 1, "active": true}));
        let result = apply_one(&config, msg).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().payload, serde_json::json!(1));

        // Filtered out
        let msg2 = make_msg(serde_json::json!({"id": 2, "active": false}));
        let result2 = apply_one(&config, msg2).unwrap();
        assert!(result2.is_none());
    }

    #[test]
    fn test_config_parse_from_pipeline_config() {
        let config = serde_json::json!({
            "transform": {
                "filter": "active",
                "payload": "id"
            }
        });
        let tc = TransformConfig::from_pipeline_config(&config);
        assert_eq!(tc.filter.as_deref(), Some("active"));
        assert_eq!(tc.payload_expr.as_deref(), Some("id"));
        assert!(!tc.is_identity());
    }

    #[test]
    fn test_config_missing_transform_is_identity() {
        let config = serde_json::json!({});
        let tc = TransformConfig::from_pipeline_config(&config);
        assert!(tc.is_identity());
    }
}
