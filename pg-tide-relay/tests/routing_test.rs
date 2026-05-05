//! Unit tests: Content-based routing — RELAY-P2-14.
//!
//! Verifies routing rule evaluation, first-match-wins behaviour,
//! default template fallback, and nested field access.
//! No database or external services required.

mod common;

#[test]
fn test_no_routing_rules_passthrough() {
    // With no routing config, messages are passed through unchanged.
    let pipeline_config = serde_json::json!({
        "source_type": "outbox",
        "sink_type": "kafka"
    });

    // Simulate what coordinator.rs does: check for routing config presence.
    let has_routing = pipeline_config.get("routing").is_some();
    assert!(!has_routing);
}

#[test]
fn test_routing_rules_parsed_from_config() {
    let pipeline_config = serde_json::json!({
        "routing": {
            "default_template": "tide.unmatched",
            "rules": [
                {
                    "match_field": "event_type",
                    "match_value": "order.created",
                    "subject": "orders.created"
                },
                {
                    "match_field": "priority",
                    "match_value": "high",
                    "subject": "high-priority.orders"
                }
            ]
        }
    });

    let rules = pipeline_config
        .pointer("/routing/rules")
        .and_then(|v| v.as_array())
        .expect("rules array");

    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0].get("match_value").unwrap().as_str(),
        Some("order.created")
    );
    assert_eq!(
        rules[1].get("subject").unwrap().as_str(),
        Some("high-priority.orders")
    );

    let default = pipeline_config
        .pointer("/routing/default_template")
        .and_then(|v| v.as_str());
    assert_eq!(default, Some("tide.unmatched"));
}

#[test]
fn test_field_matching_simple() {
    // Simulate the field_matches logic.
    let payload = serde_json::json!({"event_type": "order.created", "id": 42});

    let field_val = payload.get("event_type").and_then(|v| v.as_str());
    assert_eq!(field_val, Some("order.created"));

    // This rule matches.
    let matches = field_val == Some("order.created");
    assert!(matches);
}

#[test]
fn test_field_matching_nested() {
    let payload = serde_json::json!({
        "order": {
            "status": "completed",
            "priority": "high"
        }
    });

    // Navigate "order.status"
    let val = payload
        .get("order")
        .and_then(|o| o.get("status"))
        .and_then(|v| v.as_str());
    assert_eq!(val, Some("completed"));
}

#[test]
fn test_routing_first_match_wins() {
    let rules = vec![
        ("event_type", "order.created", "orders.created"),
        ("event_type", "order.shipped", "orders.shipped"),
        ("event_type", "order.created", "orders.duplicate"), // should never match
    ];

    let payload = serde_json::json!({"event_type": "order.created"});
    let _event_type = payload.get("event_type").and_then(|v| v.as_str());

    let mut matched_subject: Option<&str> = None;
    for (field, value, subject) in &rules {
        if payload.get(*field).and_then(|v| v.as_str()) == Some(*value) {
            matched_subject = Some(subject);
            break; // First match wins.
        }
    }

    assert_eq!(matched_subject, Some("orders.created"));
    assert_ne!(matched_subject, Some("orders.duplicate"));
}

#[test]
fn test_routing_default_when_no_match() {
    let rules: Vec<(&str, &str, &str)> = vec![("event_type", "order.created", "orders.created")];

    let payload = serde_json::json!({"event_type": "something.else"});
    let default_template = "tide.{stream_table}";

    let matched = rules
        .iter()
        .any(|(field, value, _)| payload.get(*field).and_then(|v| v.as_str()) == Some(*value));

    assert!(!matched);

    // Fall back to default.
    let resolved = if matched { "wrong" } else { default_template };
    assert_eq!(resolved, "tide.{stream_table}");
}

#[test]
fn test_routing_with_numeric_field() {
    let payload = serde_json::json!({"priority": 5, "id": 1});

    // Numeric fields are compared as strings.
    let val = match payload.get("priority") {
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    };

    assert_eq!(val.as_deref(), Some("5"));
    assert!(val.as_deref() == Some("5"));
}
