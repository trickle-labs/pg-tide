# Stability Guarantees

> **Version:** v0.33.0 (final pre-GA release)  
> **Audience:** Operators, application developers, and downstream crate authors
> who want to understand what they can rely on staying stable across pg_tide
> releases.

---

## What is stable in v1.0.0+

The following API surfaces are **guaranteed stable** from v1.0.0 onwards.
Breaking changes to these surfaces will only occur in a new major version (v2.x)
and will be announced at least one full release cycle in advance.

### SQL function signatures (v2 forms)

All SQL functions ending in `_v2` are stable.  The JSONB parameter format and
the set of documented keys are stable.  New optional keys may be added without
a breaking change.

| Stable function | Description |
|---|---|
| `tide.relay_set_outbox_v2(config JSONB)` | Configure a forward relay pipeline |
| `tide.relay_set_inbox_v2(config JSONB)` | Configure a reverse relay pipeline |
| `tide.outbox_create(name TEXT, ...)` | Create an outbox |
| `tide.outbox_publish(name TEXT, payload JSONB, dedup_key TEXT)` | Publish a message |
| `tide.outbox_status(name TEXT)` | Outbox status and lag |
| `tide.outbox_commit_offset(group_name TEXT, consumer_id TEXT, offset BIGINT)` | Advance consumer offset |
| `tide.inbox_create(name TEXT)` | Create an inbox |
| `tide.inbox_mark_processed(name TEXT, event_id TEXT)` | Mark a message processed |
| `tide.inbox_mark_failed(name TEXT, event_id TEXT, reason TEXT)` | Mark a message failed |
| `tide.inbox_status(name TEXT)` | Inbox status |
| `tide.relay_enable(name TEXT)` | Enable a pipeline |
| `tide.relay_disable(name TEXT)` | Disable a pipeline |
| `tide.relay_delete(name TEXT)` | Delete a pipeline |
| `tide.relay_list_configs()` | List all pipeline configurations |
| `tide.outbox_encryption_config(outbox_name TEXT, kms_provider TEXT, key_id TEXT, algorithm TEXT)` | Configure KMS encryption (v1.0.0) |

### Catalog table schemas

The following catalog table column names and types are stable:

| Table | Stable columns |
|---|---|
| `tide.tide_outbox_config` | `outbox_name`, `enabled`, `partition_strategy`, `retention_partitions`, `description`, `created_at` |
| `tide.tide_inbox_config` | `inbox_name`, `enabled`, `created_at` |
| `tide.relay_outbox_config` | `name`, `enabled`, `config`, `tenant_id`, `db_role`, `created_at` |
| `tide.relay_inbox_config` | `name`, `enabled`, `config`, `tenant_id`, `db_role`, `created_at` |
| `tide.relay_dlq` | `pipeline_name`, `dedup_key`, `payload`, `error_message`, `error_class`, `created_at` |
| `tide.relay_delivery_receipts` | `pipeline_name`, `outbox_name`, `message_id`, `dedup_key`, `delivered_at`, `sink_type` |
| `tide.outbox_encryption_config` | `outbox_name`, `kms_provider`, `key_id`, `algorithm`, `created_at`, `updated_at` |

New columns may be added to any catalog table in a minor release without a
breaking change.  Existing columns will not be removed or renamed in a minor release.

### Prometheus metric names

The following metric names are stable from v1.0.0:

| Metric | Type | Labels |
|---|---|---|
| `pg_tide_relay_messages_published_total` | Counter | `pipeline`, `sink_type`, `tenant` |
| `pg_tide_relay_messages_consumed_total` | Counter | `pipeline` |
| `pg_tide_relay_consumer_lag` | Gauge | `pipeline` |
| `pg_tide_relay_pipeline_healthy` | Gauge | `pipeline`, `direction` |
| `pg_tide_relay_dlq_entries_written_total` | Counter | `pipeline` |
| `pg_tide_relay_owned_pipelines` | Gauge | `relay_group_id`, `tenant` |
| `pg_tide_relay_reconcile_duration_seconds` | Histogram | `relay_group_id` |
| `pg_tide_relay_pipeline_errors_total` | Counter | `pipeline`, `error_class` |
| `pg_tide_relay_sink_publish_duration_seconds` | Histogram | `pipeline`, `sink_type` |
| `pg_tide_relay_pool_connections` | Gauge | `state` |
| `pg_tide_relay_pool_acquire_duration_seconds` | Histogram | — |
| `pg_tide_relay_receipts_written_total` | Counter | `pipeline` |

### Configuration key names (relay TOML and JSONB catalog)

The following top-level TOML keys and JSONB catalog config keys are stable:

| Context | Key | Description |
|---|---|---|
| TOML | `postgres_url` | PostgreSQL connection string |
| TOML | `metrics_addr` | Prometheus metrics endpoint address |
| TOML | `relay_group_id` | Advisory lock group identifier |
| TOML | `max_owned_pipelines` | Maximum concurrent pipeline workers |
| TOML | `max_connections` | Maximum coordinator pool connections |
| TOML | `log_level` | Log level (error/warn/info/debug/trace) |
| TOML | `log_format` | Log format (text/json) |
| JSONB | `source_type` | Source backend type |
| JSONB | `sink_type` | Sink backend type |
| JSONB | `batch_size` | Messages per poll batch |
| JSONB | `wire_format` | Wire format identifier |
| JSONB | `enabled` | Pipeline enabled state |

### Wire format schemas

The following wire format names and their top-level envelope fields are stable:

| Wire format | Stable envelope fields |
|---|---|
| `native` | `v`, `id`, `op`, `subject`, `payload`, `dedup_key`, `committed_at` |
| `debezium-json` | `before`, `after`, `op`, `source`, `ts_ms` |
| `cloudevents` | `specversion`, `id`, `type`, `source`, `time`, `data` |
| `maxwell` | `database`, `table`, `type`, `data`, `old`, `ts` |
| `canal` | `database`, `table`, `type`, `data`, `old`, `ts` |

---

## What is NOT guaranteed stable

The following are explicitly **not** covered by the stability guarantee:

- **Internal Rust types** — any type not re-exported from the `pg_tide_relay`
  crate's public `pub` API.  The `coordinator`, `dlq`, `envelope` module internal
  types may change at any time.
- **`#[doc(hidden)]` APIs** — items annotated `#[doc(hidden)]` are excluded.
- **Migration SQL script contents** — the SQL inside `sql/pg_tide--X.Y.Z--A.B.C.sql`
  files may be refactored between releases as long as the net schema effect is
  identical.  Do not depend on the exact SQL statements.
- **Positional SQL API variants (removed in v1.0.0)** — `relay_set_outbox()` with
  6 positional parameters and `relay_set_inbox()` with 8 positional parameters are
  deprecated and will be removed in v1.0.0.  See the migration guide.
- **Grafana dashboard JSON structure** — dashboard panel IDs and layout may change
  between releases as panels are added.  Reference metric names (listed above) are
  stable; the dashboard JSON itself is not.
- **Benchmark baseline numbers** — `pg-tide-relay/benches/baseline.json` records
  performance baselines on the CI runner; absolute numbers are not contractual.

---

## Deprecation policy

Before removing a stable API:

1. A `WARNING` is emitted on every call for at least one full minor-version cycle.
2. The API is listed in the `CHANGELOG.md` under "Deprecations" for all affected releases.
3. The migration guide documents the replacement and provides a migration example.
4. The removal is listed in the next major-version migration guide.

Currently deprecated APIs (to be removed in v1.0.0):

| Deprecated form | Replacement | Warning since |
|---|---|---|
| `tide.relay_set_outbox(name, outbox, sink, config, batch_size, enabled)` | `tide.relay_set_outbox_v2(config JSONB)` | v0.30.0 |
| `tide.relay_set_inbox(name, inbox, config, batch_size, source, enabled, max_retries, idempotent)` | `tide.relay_set_inbox_v2(config JSONB)` | v0.30.0 |

---

## Stability-policy enforcement

The `just check-stability` recipe verifies:

1. All public SQL functions in `pg-tide-ext/src/` have `#[pg_extern(schema = "tide")]`
   (no accidental public exports outside the `tide` schema).
2. The Prometheus metric name list in `metrics.rs` is a superset of the names
   listed in this document (prevents silent metric renames in patch releases).

Run `just check-stability` before any release to confirm the stability contract
is not broken.
