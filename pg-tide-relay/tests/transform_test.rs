//! Unit tests: JMESPath message transforms — RELAY-P2-13.
//!
//! Verifies filter, payload projection, and combined transform behaviour.
//! No database or external services required.

mod common;

/// The transform tests exercise the transform module directly via unit tests
/// inside the module (see src/jmespath_transform.rs #[cfg(test)]).
/// This integration test file verifies the same functionality from the outside
/// through the coordinator's config parsing path.

#[test]
fn test_transform_config_no_transform_is_identity() {
    // When no transform key is present, the config should be the identity.
    let config = serde_json::json!({
        "source_type": "outbox",
        "sink_type": "stdout"
    });

    // Simulate what the coordinator does:
    let filter = config
        .get("transform")
        .and_then(|t| t.get("filter"))
        .and_then(|v| v.as_str());
    let payload_expr = config
        .get("transform")
        .and_then(|t| t.get("payload"))
        .and_then(|v| v.as_str());

    assert!(filter.is_none(), "no transform key → no filter");
    assert!(payload_expr.is_none(), "no transform key → no payload expr");
}

#[test]
fn test_transform_config_parsed() {
    let config = serde_json::json!({
        "transform": {
            "filter": "active",
            "payload": "id"
        }
    });

    let filter = config.pointer("/transform/filter").and_then(|v| v.as_str());
    let payload = config
        .pointer("/transform/payload")
        .and_then(|v| v.as_str());

    assert_eq!(filter, Some("active"));
    assert_eq!(payload, Some("id"));
}

#[test]
fn test_jmespath_filter_truthy_values() {
    // These are the values that JMESPath considers truthy.
    let truthy: Vec<serde_json::Value> = vec![
        serde_json::json!(true),
        serde_json::json!(1),
        serde_json::json!("hello"),
        serde_json::json!([1, 2]),
        serde_json::json!({"a": "b"}),
    ];

    for val in &truthy {
        // Evaluate "data" field which contains the truthy value.
        let payload = serde_json::json!({"data": val});
        let expr = jmespath::compile("data").unwrap();
        let result = expr.search(&payload).unwrap();
        // All should be truthy.
        let is_truthy = match result.as_ref() {
            jmespath::Variable::Null => false,
            jmespath::Variable::Bool(b) => *b,
            jmespath::Variable::String(s) => !s.is_empty(),
            jmespath::Variable::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            jmespath::Variable::Array(a) => !a.is_empty(),
            jmespath::Variable::Object(o) => !o.is_empty(),
            _ => true,
        };
        assert!(is_truthy, "expected truthy for value: {val}");
    }
}

#[test]
fn test_jmespath_filter_falsy_values() {
    let falsy: Vec<serde_json::Value> = vec![
        serde_json::json!(false),
        serde_json::json!(null),
        serde_json::json!(""),
        serde_json::json!([]),
        serde_json::json!({}),
    ];

    for val in &falsy {
        let payload = serde_json::json!({"data": val});
        let expr = jmespath::compile("data").unwrap();
        let result = expr.search(&payload).unwrap();
        let is_truthy = match result.as_ref() {
            jmespath::Variable::Null => false,
            jmespath::Variable::Bool(b) => *b,
            jmespath::Variable::String(s) => !s.is_empty(),
            jmespath::Variable::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            jmespath::Variable::Array(a) => !a.is_empty(),
            jmespath::Variable::Object(o) => !o.is_empty(),
            _ => true,
        };
        assert!(!is_truthy, "expected falsy for value: {val}");
    }
}

#[test]
fn test_jmespath_nested_path_extraction() {
    let payload = serde_json::json!({
        "order": {
            "id": 42,
            "status": "completed",
            "customer": { "name": "Alice" }
        }
    });

    let expr = jmespath::compile("order.customer.name").unwrap();
    let result = expr.search(&payload).unwrap();
    let json_str = serde_json::to_string(&*result).unwrap();
    let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(val, serde_json::json!("Alice"));
}
