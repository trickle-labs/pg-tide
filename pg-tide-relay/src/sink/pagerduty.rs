/// PagerDuty notification sink (RELAY-P3-N3).
///
/// Sends relay messages as PagerDuty Events API v2 incidents.
/// Each message in a batch triggers a separate PagerDuty event.
///
/// - `op = "insert"` → `severity` (configurable, default `"info"`)
/// - `op = "delete"` → `severity = "info"` with summary prefix `[delete]`
/// - Custom `severity` can be set per-pipeline: `critical | error | warning | info`
///
/// Feature-gated: only compiled with `--features pagerduty`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// PagerDuty Events API v2 endpoint.
const PAGERDUTY_API_URL: &str = "https://events.pagerduty.com/v2/enqueue";

#[cfg(feature = "pagerduty")]
use reqwest::Client;

#[cfg(feature = "pagerduty")]
pub struct PagerDutySink {
    client: Client,
    /// PagerDuty Events API v2 integration key (routing key).
    routing_key: String,
    /// Default event severity: `critical`, `error`, `warning`, or `info`.
    severity: String,
    /// Optional source string (identifies the system sending the event).
    source: Option<String>,
    /// Optional component string (identifies the component of the system).
    component: Option<String>,
}

#[cfg(feature = "pagerduty")]
impl PagerDutySink {
    pub fn new(
        routing_key: impl Into<String>,
        severity: impl Into<String>,
        source: Option<String>,
        component: Option<String>,
    ) -> Result<Self, RelayError> {
        let client = crate::http_util::secure_client_for_url(
            PAGERDUTY_API_URL,
            "pagerduty",
            std::time::Duration::from_secs(30),
            false,
            true,
        )
        .map_err(|e| RelayError::sink("pagerduty", e))?;
        Ok(Self {
            client,
            routing_key: routing_key.into(),
            severity: severity.into(),
            source,
            component,
        })
    }

    /// Build a PagerDuty Events API v2 `trigger` payload for a single message.
    fn build_event(&self, msg: &RelayMessage) -> serde_json::Value {
        let summary = format!("[{}] {} — {}", msg.op, msg.subject, msg.dedup_key);
        let severity = match msg.op.as_str() {
            "delete" => "info",
            _ => &self.severity,
        };

        let mut payload = serde_json::json!({
            "summary": summary,
            "severity": severity,
            "custom_details": msg.payload,
        });

        if let Some(ref src) = self.source {
            payload["source"] = serde_json::Value::String(src.clone());
        }
        if let Some(ref comp) = self.component {
            payload["component"] = serde_json::Value::String(comp.clone());
        }

        serde_json::json!({
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "dedup_key": msg.dedup_key,
            "payload": payload,
        })
    }
}

#[cfg(feature = "pagerduty")]
#[async_trait::async_trait]
impl super::Sink for PagerDutySink {
    fn name(&self) -> &str {
        "pagerduty"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        for msg in messages {
            let event = self.build_event(msg);

            let resp = self
                .client
                .post(PAGERDUTY_API_URL)
                .header("Content-Type", "application/json")
                .json(&event)
                .send()
                .await
                .map_err(|e| RelayError::sink("pagerduty", e))?;

            if !resp.status().is_success() {
                return Err(RelayError::SinkPublish {
                    sink: "pagerduty".to_string(),
                    source: format!("HTTP {}", resp.status()).into(),
                });
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

#[cfg(all(test, feature = "pagerduty"))]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn make_msg(op: &str, order_id: i64) -> RelayMessage {
        RelayMessage::new_forward(
            "orders",
            order_id,
            0,
            op,
            serde_json::json!({"order_id": order_id}),
            false,
            None,
            format!("orders.{op}"),
        )
    }

    #[test]
    fn test_build_event_trigger_action() {
        let sink = PagerDutySink::new("R0000000000000001", "info", None, None).unwrap();
        let msg = make_msg("insert", 1);
        let event = sink.build_event(&msg);
        assert_eq!(event["event_action"], "trigger");
        assert_eq!(event["routing_key"], "R0000000000000001");
    }

    #[test]
    fn test_dedup_key_forwarded() {
        let sink = PagerDutySink::new("RTEST", "info", None, None).unwrap();
        let msg = make_msg("insert", 42);
        let event = sink.build_event(&msg);
        assert_eq!(event["dedup_key"], "orders:42:0");
    }

    #[test]
    fn test_delete_uses_info_severity() {
        // Even with severity = "critical" configured, delete must produce "info".
        let sink = PagerDutySink::new("RTEST", "critical", None, None).unwrap();
        let msg = make_msg("delete", 99);
        let event = sink.build_event(&msg);
        assert_eq!(event["payload"]["severity"], "info");
    }

    #[test]
    fn test_insert_uses_configured_severity() {
        let sink = PagerDutySink::new("RTEST", "error", None, None).unwrap();
        let msg = make_msg("insert", 1);
        let event = sink.build_event(&msg);
        assert_eq!(event["payload"]["severity"], "error");
    }

    #[test]
    fn test_source_and_component_included() {
        let sink = PagerDutySink::new(
            "RTEST",
            "warning",
            Some("relay-prod".to_string()),
            Some("orders-svc".to_string()),
        )
        .unwrap();
        let msg = make_msg("insert", 1);
        let event = sink.build_event(&msg);
        assert_eq!(event["payload"]["source"], "relay-prod");
        assert_eq!(event["payload"]["component"], "orders-svc");
    }

    #[test]
    fn test_custom_details_contains_payload() {
        let sink = PagerDutySink::new("RTEST", "info", None, None).unwrap();
        let msg = make_msg("insert", 7);
        let event = sink.build_event(&msg);
        assert_eq!(event["payload"]["custom_details"]["order_id"], 7);
    }

    #[test]
    fn test_summary_includes_op_and_dedup_key() {
        let sink = PagerDutySink::new("RTEST", "info", None, None).unwrap();
        let msg = make_msg("insert", 5);
        let event = sink.build_event(&msg);
        let summary = event["payload"]["summary"].as_str().unwrap();
        assert!(summary.contains("insert"));
        assert!(summary.contains("orders:5:0"));
    }
}
