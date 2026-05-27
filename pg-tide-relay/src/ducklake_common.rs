/// Shared Parquet-building, column statistics, and schema-change-detection
/// logic used by both `DuckLakeSink` (PostgreSQL-backed catalog) and
/// `RockLakeSink` (RockLake PG-wire sidecar).
///
/// Extracted in v0.37.0 as part of the RockLake Integration Phases 0–1
/// scaffold described in [plans/ecosystem/rocklake.md].
///
/// # Feature gating
///
/// The Parquet-building helpers (`build_parquet_bytes`, `ColStats`,
/// `str_min_max`) require the `ducklake` OR `rocklake` feature flag,
/// because they pull in the `parquet` crate dependency.
///
/// The configuration enums (`SchemaChangePolicy`, `DuckLakePartition`,
/// `DuckLakeCompression`) are unconditionally compiled — they are small
/// and referenced in coordinator config-parsing code that does not itself
/// require the full Parquet dependency.
#[cfg(any(feature = "ducklake", feature = "rocklake"))]
use crate::error::RelayError;

// ── Configuration enums ──────────────────────────────────────────────────────

/// How the sink behaves when a breaking schema change is detected in an
/// incoming message batch.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SchemaChangePolicy {
    /// Pause the pipeline (emit a permanent error so the coordinator pauses it).
    Pause,
    /// Route the offending batch to the DLQ.
    RouteToDlq,
    /// Log a warning and continue processing.
    #[default]
    WarnAndContinue,
    /// Automatically start a new DuckLake stream / table version.
    AutoNewStream,
}

/// Partition strategy for newly created DuckLake / RockLake tables.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum DuckLakePartition {
    /// No hidden partitioning (default).
    #[default]
    None,
    /// Partition by day on `_committed_at`.
    Daily,
    /// Partition by month on `_committed_at`.
    Monthly,
    /// Bucket partitioning on `_subject` with the given bucket count.
    Bucket(u32),
}

impl DuckLakePartition {
    /// Returns the string representation stored in catalog metadata.
    pub fn as_str(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::Daily => "daily".to_string(),
            Self::Monthly => "monthly".to_string(),
            Self::Bucket(n) => format!("bucket:{n}"),
        }
    }
}

/// Parquet compression codec.
#[derive(Debug, Clone, PartialEq)]
pub enum DuckLakeCompression {
    Snappy,
    Zstd,
    None,
}

// ── Column statistics ────────────────────────────────────────────────────────

/// Per-column statistics computed from a message batch (for filter pushdown).
///
/// Requires the `ducklake` or `rocklake` feature.
#[cfg(any(feature = "ducklake", feature = "rocklake"))]
pub struct ColStats {
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub null_count: i64,
}

/// Compute the lexicographic min and max of a string iterator.
///
/// Returns `(None, None)` for an empty iterator.
#[cfg(any(feature = "ducklake", feature = "rocklake"))]
pub fn str_min_max<'a>(
    mut iter: impl Iterator<Item = &'a str>,
) -> (Option<String>, Option<String>) {
    let first = match iter.next() {
        Some(s) => s,
        None => return (None, None),
    };
    let mut min = first.to_string();
    let mut max = first.to_string();
    for s in iter {
        if s < min.as_str() {
            min = s.to_string();
        }
        if s > max.as_str() {
            max = s.to_string();
        }
    }
    (Some(min), Some(max))
}

// ── Parquet building ─────────────────────────────────────────────────────────

/// Build a Parquet file in memory from a slice of `RelayMessage` references.
///
/// Returns `(parquet_bytes, footer_size_bytes)`.
///
/// The generated Parquet schema has five columns:
/// - `_dedup_key` (BYTE_ARRAY / String, REQUIRED)
/// - `_subject`   (BYTE_ARRAY / String, REQUIRED)
/// - `_op`        (BYTE_ARRAY / String, REQUIRED)
/// - `_outbox_id` (INT64, OPTIONAL)
/// - `data`       (BYTE_ARRAY / String, REQUIRED) — JSON-serialized payload
///
/// Both `DuckLakeSink` and `RockLakeSink` use this function; the resulting
/// bytes are written to object storage and the `footer_size` is recorded in
/// `ducklake_file_column_stats`.
#[cfg(any(feature = "ducklake", feature = "rocklake"))]
pub fn build_parquet_bytes(
    messages: &[&crate::envelope::RelayMessage],
    compression: &DuckLakeCompression,
) -> Result<(Vec<u8>, i64), RelayError> {
    use std::sync::Arc;

    use parquet::basic::{
        Compression as PqCompression, LogicalType, Repetition, Type as PhysicalType, ZstdLevel,
    };
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;

    let sink_label = "ducklake_common";

    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    Type::primitive_type_builder("_dedup_key", PhysicalType::BYTE_ARRAY)
                        .with_logical_type(Some(LogicalType::String))
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .map_err(|e| RelayError::SinkPublish {
                            sink: sink_label.to_string(),
                            source: Box::new(e),
                        })?,
                ),
                Arc::new(
                    Type::primitive_type_builder("_subject", PhysicalType::BYTE_ARRAY)
                        .with_logical_type(Some(LogicalType::String))
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .map_err(|e| RelayError::SinkPublish {
                            sink: sink_label.to_string(),
                            source: Box::new(e),
                        })?,
                ),
                Arc::new(
                    Type::primitive_type_builder("_op", PhysicalType::BYTE_ARRAY)
                        .with_logical_type(Some(LogicalType::String))
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .map_err(|e| RelayError::SinkPublish {
                            sink: sink_label.to_string(),
                            source: Box::new(e),
                        })?,
                ),
                Arc::new(
                    Type::primitive_type_builder("_outbox_id", PhysicalType::INT64)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .map_err(|e| RelayError::SinkPublish {
                            sink: sink_label.to_string(),
                            source: Box::new(e),
                        })?,
                ),
                Arc::new(
                    Type::primitive_type_builder("data", PhysicalType::BYTE_ARRAY)
                        .with_logical_type(Some(LogicalType::String))
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .map_err(|e| RelayError::SinkPublish {
                            sink: sink_label.to_string(),
                            source: Box::new(e),
                        })?,
                ),
            ])
            .build()
            .map_err(|e| RelayError::SinkPublish {
                sink: sink_label.to_string(),
                source: Box::new(e),
            })?,
    );

    let pq_compression = match compression {
        DuckLakeCompression::Snappy => PqCompression::SNAPPY,
        DuckLakeCompression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        DuckLakeCompression::None => PqCompression::UNCOMPRESSED,
    };

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(pq_compression)
            .build(),
    );
    let mut buf: Vec<u8> = Vec::new();
    let cursor = std::io::Cursor::new(&mut buf);
    let mut writer =
        SerializedFileWriter::new(cursor, schema, props).map_err(|e| RelayError::SinkPublish {
            sink: sink_label.to_string(),
            source: Box::new(e),
        })?;

    let n = messages.len();
    let mut dedup_keys: Vec<ByteArray> = Vec::with_capacity(n);
    let mut subjects: Vec<ByteArray> = Vec::with_capacity(n);
    let mut ops: Vec<ByteArray> = Vec::with_capacity(n);
    let mut outbox_ids: Vec<i64> = Vec::with_capacity(n);
    let mut outbox_def: Vec<i16> = Vec::with_capacity(n);
    let mut data_vals: Vec<ByteArray> = Vec::with_capacity(n);

    for msg in messages {
        dedup_keys.push(ByteArray::from(msg.dedup_key.as_str()));
        subjects.push(ByteArray::from(msg.subject.as_str()));
        ops.push(ByteArray::from(msg.op.as_str()));
        if let Some(id) = msg.outbox_id {
            outbox_ids.push(id);
            outbox_def.push(1);
        } else {
            outbox_ids.push(0);
            outbox_def.push(0);
        }
        let data_str = serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
        data_vals.push(ByteArray::from(data_str.as_str()));
    }

    let mut row_group = writer
        .next_row_group()
        .map_err(|e| RelayError::SinkPublish {
            sink: sink_label.to_string(),
            source: Box::new(e),
        })?;

    macro_rules! write_ba_col {
        ($vals:expr) => {{
            let mut cw = row_group.next_column().unwrap().unwrap();
            cw.typed::<ByteArrayType>()
                .write_batch(&$vals, None, None)
                .map_err(|e| RelayError::SinkPublish {
                    sink: sink_label.to_string(),
                    source: Box::new(e),
                })?;
            cw.close().map_err(|e| RelayError::SinkPublish {
                sink: sink_label.to_string(),
                source: Box::new(e),
            })?;
        }};
    }

    write_ba_col!(dedup_keys);
    write_ba_col!(subjects);
    write_ba_col!(ops);

    {
        let mut cw = row_group.next_column().unwrap().unwrap();
        cw.typed::<Int64Type>()
            .write_batch(&outbox_ids, Some(&outbox_def), None)
            .map_err(|e| RelayError::SinkPublish {
                sink: sink_label.to_string(),
                source: Box::new(e),
            })?;
        cw.close().map_err(|e| RelayError::SinkPublish {
            sink: sink_label.to_string(),
            source: Box::new(e),
        })?;
    }

    write_ba_col!(data_vals);

    row_group.close().map_err(|e| RelayError::SinkPublish {
        sink: sink_label.to_string(),
        source: Box::new(e),
    })?;
    let _metadata = writer.close().map_err(|e| RelayError::SinkPublish {
        sink: sink_label.to_string(),
        source: Box::new(e),
    })?;

    // Compute Parquet footer size from the last 8 bytes of the file
    // (4-byte footer length + 4-byte magic "PAR1").
    let footer_size = if buf.len() >= 8 {
        let len_bytes: [u8; 4] = buf[buf.len() - 8..buf.len() - 4]
            .try_into()
            .unwrap_or([0; 4]);
        i32::from_le_bytes(len_bytes) as i64
    } else {
        0i64
    };

    Ok((buf, footer_size))
}
