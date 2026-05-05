/// Content-based routing (RELAY-P2-14).
///
/// Evaluates payload-based routing rules to determine the subject/topic for
/// each message, allowing dynamic routing to different destinations based on
/// message content.
///
/// Rules are evaluated in order; the first match wins. A default template
/// is used when no rule matches.
///
/// Configuration in the pipeline's `config` JSONB column:
///
/// ```json
/// {
///   "routing": {
///     "default_template": "tide.{stream_table}",
///     "rules": [
///       { "match_field": "event_type", "match_value": "order.created", "subject": "orders.created" },
///       { "match_field": "priority",   "match_value": "high",           "subject": "high-priority.{stream_table}" }
///     ]
///   }
/// }
/// ```
///
/// `match_field` is a simple dot-separated path into the payload.
/// `match_value` is the string to compare against (equality check).
/// `subject` is the output subject template (supports `{stream_table}`, `{op}`, `{outbox_id}`).
use crate::envelope::RelayMessage;
use crate::transforms::render_subject;

/// A single routing rule: if the field matches the value, use this subject.
#[derive(Debug, Clone)]
pub struct RoutingRule {
    /// Dot-separated path into the payload (e.g. `"event_type"` or `"order.status"`).
    pub match_field: String,
    /// Expected value (string equality check).
    pub match_value: String,
    /// Subject template (supports same variables as `render_subject`).
    pub subject: String,
}

/// Content-based routing configuration for a pipeline.
#[derive(Debug, Clone, Default)]
pub struct RoutingConfig {
    /// Ordered list of routing rules (first match wins).
    pub rules: Vec<RoutingRule>,
    /// Default subject template when no rule matches.
    pub default_template: String,
}

impl RoutingConfig {
    /// Parse routing config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let r = match config.get("routing") {
            Some(r) => r,
            None => return Self::default(),
        };

        let default_template = r
            .get("default_template")
            .and_then(|v| v.as_str())
            .unwrap_or("{stream_table}.{op}")
            .to_string();

        let rules = r
            .get("rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|rule| {
                        let match_field = rule.get("match_field")?.as_str()?.to_string();
                        let match_value = rule.get("match_value")?.as_str()?.to_string();
                        let subject = rule.get("subject")?.as_str()?.to_string();
                        Some(RoutingRule {
                            match_field,
                            match_value,
                            subject,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            rules,
            default_template,
        }
    }

    /// Returns true if no routing rules are configured (use message subject as-is).
    pub fn is_passthrough(&self) -> bool {
        self.rules.is_empty()
    }
}

/// Resolve the subject for a message using the routing rules.
///
/// If a rule matches, the rule's `subject` template is rendered.
/// If no rule matches, the `default_template` is rendered.
/// If routing config is empty, the message's existing subject is returned unchanged.
pub fn resolve_subject(config: &RoutingConfig, msg: &RelayMessage) -> String {
    if config.is_passthrough() {
        return msg.subject.clone();
    }

    for rule in &config.rules {
        if field_matches(&msg.payload, &rule.match_field, &rule.match_value) {
            let stream_table = extract_stream_table(msg);
            return render_subject(
                &rule.subject,
                &stream_table,
                &msg.op,
                msg.outbox_id.unwrap_or(0),
                msg.refresh_id,
            );
        }
    }

    // No rule matched — use default template.
    let stream_table = extract_stream_table(msg);
    render_subject(
        &config.default_template,
        &stream_table,
        &msg.op,
        msg.outbox_id.unwrap_or(0),
        msg.refresh_id,
    )
}

/// Apply routing to an entire batch, updating each message's subject in place.
pub fn apply_routing(config: &RoutingConfig, messages: &mut [RelayMessage]) {
    if config.is_passthrough() {
        return;
    }
    for msg in messages.iter_mut() {
        msg.subject = resolve_subject(config, msg);
    }
}

/// Extract the stream_table name from the message subject or payload.
fn extract_stream_table(msg: &RelayMessage) -> String {
    // Try subject first (it may already contain the stream table name from forward mode).
    if !msg.subject.is_empty() && !msg.subject.contains('.') {
        return msg.subject.clone();
    }
    // Fall back to `stream_table` field in payload.
    msg.payload
        .get("stream_table")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Check whether a dot-separated field path in `payload` equals `expected_value`.
fn field_matches(payload: &serde_json::Value, field_path: &str, expected_value: &str) -> bool {
    let value = get_field(payload, field_path);
    match value {
        Some(serde_json::Value::String(s)) => s == expected_value,
        Some(serde_json::Value::Number(n)) => n.to_string() == expected_value,
        Some(serde_json::Value::Bool(b)) => b.to_string() == expected_value,
        _ => false,
    }
}

/// Navigate a dot-separated path into a JSON value.
fn get_field<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path.split('.') {
        current = current.get(key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn make_msg(subject: &str, op: &str, payload: serde_json::Value) -> RelayMessage {
        let mut msg = RelayMessage::new_reverse("key", "event", payload);
        msg.subject = subject.to_string();
        msg.op = op.to_string();
        msg
    }

    #[test]
    fn test_passthrough_when_no_rules() {
        let config = RoutingConfig::default();
        let msg = make_msg("orders.insert", "insert", serde_json::json!({}));
        let subject = resolve_subject(&config, &msg);
        assert_eq!(subject, "orders.insert");
    }

    #[test]
    fn test_first_matching_rule_wins() {
        let config = RoutingConfig {
            rules: vec![
                RoutingRule {
                    match_field: "event_type".to_string(),
                    match_value: "order.created".to_string(),
                    subject: "orders.created".to_string(),
                },
                RoutingRule {
                    match_field: "event_type".to_string(),
                    match_value: "order.shipped".to_string(),
                    subject: "orders.shipped".to_string(),
                },
            ],
            default_template: "tide.default".to_string(),
        };

        let msg1 = make_msg(
            "default",
            "insert",
            serde_json::json!({"event_type": "order.created"}),
        );
        assert_eq!(resolve_subject(&config, &msg1), "orders.created");

        let msg2 = make_msg(
            "default",
            "insert",
            serde_json::json!({"event_type": "order.shipped"}),
        );
        assert_eq!(resolve_subject(&config, &msg2), "orders.shipped");
    }

    #[test]
    fn test_default_template_when_no_rule_matches() {
        let config = RoutingConfig {
            rules: vec![RoutingRule {
                match_field: "event_type".to_string(),
                match_value: "order.created".to_string(),
                subject: "orders.created".to_string(),
            }],
            default_template: "tide.unmatched".to_string(),
        };

        let msg = make_msg(
            "default",
            "insert",
            serde_json::json!({"event_type": "other"}),
        );
        assert_eq!(resolve_subject(&config, &msg), "tide.unmatched");
    }

    #[test]
    fn test_nested_field_matching() {
        let config = RoutingConfig {
            rules: vec![RoutingRule {
                match_field: "order.status".to_string(),
                match_value: "completed".to_string(),
                subject: "orders.completed".to_string(),
            }],
            default_template: "orders.other".to_string(),
        };

        let msg = make_msg(
            "default",
            "update",
            serde_json::json!({"order": {"status": "completed"}}),
        );
        assert_eq!(resolve_subject(&config, &msg), "orders.completed");
    }

    #[test]
    fn test_parse_routing_config_from_pipeline_config() {
        let config = serde_json::json!({
            "routing": {
                "default_template": "tide.default",
                "rules": [
                    {
                        "match_field": "event_type",
                        "match_value": "order.created",
                        "subject": "orders.created"
                    }
                ]
            }
        });
        let rc = RoutingConfig::from_pipeline_config(&config);
        assert!(!rc.is_passthrough());
        assert_eq!(rc.rules.len(), 1);
        assert_eq!(rc.rules[0].match_value, "order.created");
        assert_eq!(rc.default_template, "tide.default");
    }

    #[test]
    fn test_apply_routing_modifies_subjects() {
        let config = RoutingConfig {
            rules: vec![RoutingRule {
                match_field: "event_type".to_string(),
                match_value: "order.created".to_string(),
                subject: "orders.created".to_string(),
            }],
            default_template: "orders.other".to_string(),
        };

        let mut messages = vec![
            make_msg(
                "default",
                "insert",
                serde_json::json!({"event_type": "order.created"}),
            ),
            make_msg(
                "default",
                "insert",
                serde_json::json!({"event_type": "order.updated"}),
            ),
        ];
        apply_routing(&config, &mut messages);
        assert_eq!(messages[0].subject, "orders.created");
        assert_eq!(messages[1].subject, "orders.other");
    }
}
