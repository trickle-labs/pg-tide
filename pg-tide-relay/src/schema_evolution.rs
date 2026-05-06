/// Schema evolution guardrails (v0.13.0).
///
/// Stores schema fingerprints per pipeline in `tide.relay_schema_fingerprints`.
/// On each message batch, compares the current schema fingerprint with the stored
/// one to classify changes as additive or breaking, then applies the configured
/// `on_schema_change` policy.
///
/// ## Policies
/// - `warn`     — log a warning, continue processing (default)
/// - `continue` — silently continue
/// - `pause`    — return an error, pausing the pipeline
/// - `dlq`      — route the batch to DLQ
use std::collections::HashMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio_postgres::Client;

use crate::error::RelayError;

/// Policy applied when a schema change is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnSchemaChange {
    /// Log a warning and continue (default).
    #[default]
    Warn,
    /// Continue without logging.
    Continue,
    /// Pause the pipeline (return error).
    Pause,
    /// Route batch to DLQ.
    Dlq,
}

impl OnSchemaChange {
    pub fn parse_config(s: &str) -> Self {
        s.parse().unwrap_or_default()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Continue => "continue",
            Self::Pause => "pause",
            Self::Dlq => "dlq",
        }
    }
}

impl std::str::FromStr for OnSchemaChange {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "warn" => Self::Warn,
            "continue" => Self::Continue,
            "pause" => Self::Pause,
            "dlq" => Self::Dlq,
            _ => Self::Warn,
        })
    }
}

/// Schema change classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaChangeKind {
    /// First time seeing this topic.
    Initial,
    /// New columns added — backward-compatible.
    Additive,
    /// Columns removed or types changed — potentially breaking.
    Breaking,
    /// No change.
    NoChange,
}

/// Per-topic schema fingerprint.
#[derive(Debug, Clone)]
pub struct SchemaFingerprint {
    pub fingerprint: String,
    pub column_names: Vec<String>,
    pub on_schema_change: OnSchemaChange,
}

/// Computes a deterministic SHA-256 fingerprint from a sorted list of column names.
pub fn compute_fingerprint(columns: &[String]) -> String {
    let mut sorted = columns.to_vec();
    sorted.sort_unstable();
    let joined = sorted.join(",");
    let hash = Sha256::digest(joined.as_bytes());
    hex::encode(hash)
}

/// Classify a schema change given the old and new column sets.
pub fn classify_change(old_columns: &[String], new_columns: &[String]) -> SchemaChangeKind {
    let old_set: std::collections::HashSet<_> = old_columns.iter().collect();
    let new_set: std::collections::HashSet<_> = new_columns.iter().collect();

    if old_set == new_set {
        return SchemaChangeKind::NoChange;
    }

    // Check if any old columns are missing (breaking removal).
    let removed: Vec<_> = old_set.difference(&new_set).collect();
    if !removed.is_empty() {
        return SchemaChangeKind::Breaking;
    }

    // All old columns present — only additions.
    SchemaChangeKind::Additive
}

/// Schema evolution guard for a pipeline.
///
/// Tracks per-topic fingerprints in memory (warm cache) and persists them
/// to `tide.relay_schema_fingerprints` for durability across restarts.
pub struct SchemaEvolutionGuard {
    pipeline_name: String,
    db: Arc<Client>,
    /// In-memory cache: topic → fingerprint.
    cache: HashMap<String, SchemaFingerprint>,
    default_policy: OnSchemaChange,
}

impl SchemaEvolutionGuard {
    pub fn new(
        pipeline_name: impl Into<String>,
        db: Arc<Client>,
        default_policy: OnSchemaChange,
    ) -> Self {
        Self {
            pipeline_name: pipeline_name.into(),
            db,
            cache: HashMap::new(),
            default_policy,
        }
    }

    /// Build from pipeline config JSON.
    pub fn from_config(
        pipeline_name: impl Into<String>,
        db: Arc<Client>,
        config: &serde_json::Value,
    ) -> Self {
        let policy_str = config
            .get("on_schema_change")
            .and_then(|v| v.as_str())
            .unwrap_or("warn");
        Self::new(pipeline_name, db, OnSchemaChange::parse_config(policy_str))
    }

    /// Observe the schema of a message batch.
    ///
    /// Extracts column names from the first message's payload keys and checks
    /// against the stored fingerprint.  Returns the change kind and policy.
    pub async fn observe(
        &mut self,
        topic: &str,
        columns: &[String],
    ) -> Result<(SchemaChangeKind, OnSchemaChange), RelayError> {
        let new_fingerprint = compute_fingerprint(columns);

        if let Some(stored) = self.cache.get(topic) {
            if stored.fingerprint == new_fingerprint {
                return Ok((SchemaChangeKind::NoChange, stored.on_schema_change));
            }
            let kind = classify_change(&stored.column_names, columns);
            let policy = stored.on_schema_change;

            // Update cache.
            self.cache.insert(
                topic.to_string(),
                SchemaFingerprint {
                    fingerprint: new_fingerprint.clone(),
                    column_names: columns.to_vec(),
                    on_schema_change: policy,
                },
            );

            // Persist to DB (best-effort).
            let _ = self
                .upsert_fingerprint(topic, &new_fingerprint, columns, policy)
                .await;

            return Ok((kind, policy));
        }

        // First time — load from DB or treat as initial.
        let existing = self.load_from_db(topic).await.ok().flatten();
        let (kind, policy) = match existing {
            Some(ref fp) if fp.fingerprint == new_fingerprint => {
                // Matches DB copy — no change.
                self.cache.insert(topic.to_string(), fp.clone());
                return Ok((SchemaChangeKind::NoChange, fp.on_schema_change));
            }
            Some(ref fp) => {
                let kind = classify_change(&fp.column_names, columns);
                (kind, fp.on_schema_change)
            }
            None => (SchemaChangeKind::Initial, self.default_policy),
        };

        self.cache.insert(
            topic.to_string(),
            SchemaFingerprint {
                fingerprint: new_fingerprint.clone(),
                column_names: columns.to_vec(),
                on_schema_change: policy,
            },
        );

        let _ = self
            .upsert_fingerprint(topic, &new_fingerprint, columns, policy)
            .await;

        Ok((kind, policy))
    }

    async fn load_from_db(&self, topic: &str) -> Result<Option<SchemaFingerprint>, RelayError> {
        let row = self
            .db
            .query_opt(
                "SELECT fingerprint, column_names, on_schema_change \
                 FROM tide.relay_schema_fingerprints \
                 WHERE pipeline_name = $1 AND topic = $2",
                &[&self.pipeline_name, &topic],
            )
            .await?;

        Ok(row.map(|r| {
            let fingerprint: String = r.get(0);
            let column_names: Vec<String> = r.get(1);
            let policy_str: String = r.get(2);
            SchemaFingerprint {
                fingerprint,
                column_names,
                on_schema_change: OnSchemaChange::parse_config(&policy_str),
            }
        }))
    }

    async fn upsert_fingerprint(
        &self,
        topic: &str,
        fingerprint: &str,
        columns: &[String],
        policy: OnSchemaChange,
    ) -> Result<(), RelayError> {
        let col_count = columns.len() as i32;
        self.db
            .execute(
                "INSERT INTO tide.relay_schema_fingerprints \
                 (pipeline_name, topic, fingerprint, column_count, column_names, on_schema_change, last_seen_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, now()) \
                 ON CONFLICT (pipeline_name, topic) DO UPDATE \
                 SET fingerprint = EXCLUDED.fingerprint, \
                     column_count = EXCLUDED.column_count, \
                     column_names = EXCLUDED.column_names, \
                     last_seen_at = now()",
                &[
                    &self.pipeline_name,
                    &topic,
                    &fingerprint,
                    &col_count,
                    &columns,
                    &policy.as_str(),
                ],
            )
            .await?;
        Ok(())
    }
}

/// Extract column names from a JSON object payload.
/// Returns an empty vec for non-object payloads.
pub fn extract_columns(payload: &serde_json::Value) -> Vec<String> {
    match payload.as_object() {
        Some(obj) => obj.keys().cloned().collect(),
        None => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_fingerprint_deterministic() {
        let cols1 = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        let cols2 = vec!["email".to_string(), "id".to_string(), "name".to_string()];
        // Different order, same content → same fingerprint.
        assert_eq!(compute_fingerprint(&cols1), compute_fingerprint(&cols2));
    }

    #[test]
    fn test_compute_fingerprint_different_for_different_cols() {
        let cols1 = vec!["id".to_string(), "name".to_string()];
        let cols2 = vec!["id".to_string(), "name".to_string(), "extra".to_string()];
        assert_ne!(compute_fingerprint(&cols1), compute_fingerprint(&cols2));
    }

    #[test]
    fn test_classify_no_change() {
        let cols = vec!["id".to_string(), "name".to_string()];
        assert_eq!(classify_change(&cols, &cols), SchemaChangeKind::NoChange);
    }

    #[test]
    fn test_classify_additive() {
        let old = vec!["id".to_string(), "name".to_string()];
        let new = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        assert_eq!(classify_change(&old, &new), SchemaChangeKind::Additive);
    }

    #[test]
    fn test_classify_breaking() {
        let old = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        let new = vec!["id".to_string(), "name".to_string()]; // email removed
        assert_eq!(classify_change(&old, &new), SchemaChangeKind::Breaking);
    }

    #[test]
    fn test_on_schema_change_roundtrip() {
        for s in &["warn", "continue", "pause", "dlq"] {
            let p = OnSchemaChange::parse_config(s);
            assert_eq!(p.as_str(), *s);
        }
    }

    #[test]
    fn test_extract_columns_from_object() {
        let payload = serde_json::json!({"id": 1, "name": "Alice"});
        let mut cols = extract_columns(&payload);
        cols.sort();
        assert_eq!(cols, vec!["id", "name"]);
    }

    #[test]
    fn test_extract_columns_from_non_object() {
        let payload = serde_json::json!([1, 2, 3]);
        assert_eq!(extract_columns(&payload), Vec::<String>::new());
    }
}
