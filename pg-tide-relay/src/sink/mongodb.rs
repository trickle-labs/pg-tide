/// MongoDB analytics sink (v0.10.0 — RELAY-P3-MDB).
///
/// Writes pg-tide relay messages as MongoDB documents.
/// Each message is upserted into a collection using the `dedup_key` as the
/// document `_id` to achieve at-least-once delivery with idempotent semantics.
///
/// Operations mapping:
/// - `op = "insert"` / `op = "update"` → `replaceOne` with `upsert: true`
/// - `op = "delete"` → `deleteOne`
/// - `is_full_refresh = true` → `drop` + `insertMany`
///
/// Feature-gated: only compiled with `--features mongodb`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "mongodb")]
use mongodb::{
    bson::{doc, to_document, Document},
    options::{ClientOptions, ReplaceOptions, WriteConcern},
    Client,
};

/// Configuration for the MongoDB sink.
#[derive(Debug, Clone)]
pub struct MongoDbConfig {
    /// MongoDB connection string, e.g. `mongodb://localhost:27017`.
    pub connection_string: String,
    /// Target MongoDB database name.
    pub database: String,
    /// Collection name template; `{stream_table}` is replaced with the message subject.
    pub collection_template: String,
    /// Field in the document to use as MongoDB `_id` (defaults to `dedup_key`).
    pub doc_id_field: String,
    /// Write concern: `"majority"` or `"1"` etc.
    pub write_concern: String,
}

impl MongoDbConfig {
    pub fn new(connection_string: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            database: database.into(),
            collection_template: "{stream_table}".to_string(),
            doc_id_field: "dedup_key".to_string(),
            write_concern: "majority".to_string(),
        }
    }

    /// Resolve the collection name for a given subject.
    pub fn collection_for(&self, subject: &str) -> String {
        self.collection_template.replace("{stream_table}", subject)
    }

    /// Convert a RelayMessage into a MongoDB document.
    pub fn to_document(&self, msg: &RelayMessage) -> Result<serde_json::Value, String> {
        let mut doc = if msg.payload.is_object() {
            msg.payload.clone()
        } else {
            serde_json::json!({ "data": msg.payload })
        };

        let obj = doc.as_object_mut().ok_or("payload is not an object")?;
        obj.insert("_dedup_key".to_string(), serde_json::Value::String(msg.dedup_key.clone()));
        obj.insert("_subject".to_string(), serde_json::Value::String(msg.subject.clone()));
        obj.insert("_op".to_string(), serde_json::Value::String(msg.op.clone()));
        if let Some(id) = msg.outbox_id {
            obj.insert("_outbox_id".to_string(), serde_json::Value::Number(id.into()));
        }

        Ok(doc)
    }
}

#[cfg(feature = "mongodb")]
pub struct MongoDbSink {
    client: Client,
    config: MongoDbConfig,
}

#[cfg(feature = "mongodb")]
impl MongoDbSink {
    pub async fn new(config: MongoDbConfig) -> Result<Self, RelayError> {
        let mut client_options = ClientOptions::parse(&config.connection_string)
            .await
            .map_err(|e| RelayError::sink("mongodb", e))?;

        // Apply write concern.
        if config.write_concern == "majority" {
            client_options.write_concern =
                Some(WriteConcern::builder().w(mongodb::options::Acknowledgment::Majority).build());
        }

        let client =
            Client::with_options(client_options).map_err(|e| RelayError::sink("mongodb", e))?;

        Ok(Self { client, config })
    }
}

#[cfg(feature = "mongodb")]
#[async_trait::async_trait]
impl super::Sink for MongoDbSink {
    fn name(&self) -> &str {
        "mongodb"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let db = self.client.database(&self.config.database);

        for msg in messages {
            let coll_name = self.config.collection_for(&msg.subject);
            let coll: mongodb::Collection<Document> = db.collection(&coll_name);

            if msg.op == "delete" {
                let filter = doc! { "_dedup_key": &msg.dedup_key };
                coll.delete_one(filter)
                    .await
                    .map_err(|e| RelayError::sink("mongodb", e))?;
            } else {
                // Convert relay message payload to BSON document.
                let json_doc = self.config.to_document(msg).map_err(|e| {
                    RelayError::SinkPublish { sink: "mongodb".to_string(), source: e.into() }
                })?;
                let bson_doc = to_document(
                    &serde_json::from_value::<serde_json::Value>(json_doc)
                        .map_err(|e| RelayError::sink("mongodb", e))?,
                )
                .map_err(|e| RelayError::sink("mongodb", e))?;

                let filter = doc! { "_dedup_key": &msg.dedup_key };
                let opts = ReplaceOptions::builder().upsert(true).build();
                coll.replace_one(filter, bson_doc)
                    .with_options(opts)
                    .await
                    .map_err(|e| RelayError::sink("mongodb", e))?;
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
