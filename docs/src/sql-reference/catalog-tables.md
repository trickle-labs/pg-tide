# Catalog Tables

pg_tide stores all state in relational tables within the `tide` schema. This page documents the underlying tables that power the SQL API.

---

## tide.tide_outbox_config

One row per named outbox.

| Column | Type | Description |
|--------|------|-------------|
| `outbox_name` | TEXT (PK) | Unique outbox identifier |
| `retention_hours` | INT | Hours to retain consumed messages |
| `inline_threshold` | INT | Deprecated compatibility value; not a native pending-row cap |
| `enabled` | BOOLEAN | Whether publishing is allowed |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

---

## tide.tide_outbox_messages

Shared message store for all outboxes.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT (PK) | Auto-incrementing message ID |
| `outbox_name` | TEXT (FK) | Which outbox this belongs to |
| `payload` | JSONB | Message body |
| `headers` | JSONB | Message metadata |
| `created_at` | TIMESTAMPTZ | Publication time |
| `consumed_at` | TIMESTAMPTZ | Legacy/global-consumer status; not authoritative for native delivery |
| `consumer_group` | TEXT | Which group consumed it |

**Indexes:**
- `idx_tide_outbox_messages_poll` — unconditional `(outbox_name, id)` index for native relay polling
- cleanup-supporting indexes selected by the operational query plans

The parent remains public when optional ID-range partitions are enabled. Child
names are numeric bounds, not logical outbox names.

---

## tide.tide_consumer_groups

Named consumer groups with offset reset policy.

| Column | Type | Description |
|--------|------|-------------|
| `group_name` | TEXT (PK) | Unique group name |
| `outbox_name` | TEXT (FK) | Outbox being consumed |
| `auto_offset_reset` | TEXT | `earliest`, `latest`, or `none` |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

---

## tide.tide_consumer_offsets

Per-consumer committed offsets and heartbeats.

| Column | Type | Description |
|--------|------|-------------|
| `group_name` | TEXT (PK, FK) | Consumer group |
| `consumer_id` | TEXT (PK) | Consumer instance identifier |
| `committed_offset` | BIGINT | Last processed message ID |
| `last_heartbeat` | TIMESTAMPTZ | Last liveness signal |

---

## tide.tide_consumer_leases

Visibility leases for in-flight message batches.

| Column | Type | Description |
|--------|------|-------------|
| `group_name` | TEXT (PK, FK) | Consumer group |
| `consumer_id` | TEXT (PK, FK) | Consumer instance |
| `lease_start` | BIGINT | First message ID in the leased batch |
| `lease_end` | BIGINT | Last message ID in the leased batch |
| `expires_at` | TIMESTAMPTZ | When the lease expires |

---

## tide.tide_inbox_config

Named inbox configurations.

| Column | Type | Description |
|--------|------|-------------|
| `inbox_name` | TEXT (PK) | Unique inbox identifier |
| `inbox_schema` | TEXT | Schema containing the inbox table |
| `max_retries` | INT | Attempts before DLQ |
| `processed_retention_hours` | INT | Hours to keep processed messages |
| `dlq_retention_hours` | INT | Hours to keep DLQ messages |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

---

## tide.relay_outbox_config

Forward relay pipeline definitions.

| Column | Type | Description |
|--------|------|-------------|
| `name` | TEXT (PK) | Unique pipeline name |
| `enabled` | BOOLEAN | Whether the pipeline is active |
| `config` | JSONB | Full pipeline config (outbox, sink, params) |

**Triggers:** `relay_outbox_config_notify` — fires `pg_notify('tide_relay_config', ...)` on changes.

---

## tide.relay_inbox_config

Reverse relay pipeline definitions.

| Column | Type | Description |
|--------|------|-------------|
| `name` | TEXT (PK) | Unique pipeline name |
| `enabled` | BOOLEAN | Whether the pipeline is active |
| `config` | JSONB | Full pipeline config (inbox, source, params) |

**Triggers:** `relay_inbox_config_notify` — fires `pg_notify('tide_relay_config', ...)` on changes.

---

## tide.relay_consumer_offsets

Durable per-pipeline offset tracking for the relay binary.

| Column | Type | Description |
|--------|------|-------------|
| `relay_group_id` | TEXT (PK) | Relay deployment group |
| `pipeline_id` | TEXT (PK) | Pipeline name |
| `outbox_name` | TEXT (PK) | Logical outbox scope |
| `last_change_id` | BIGINT | Highest acknowledged message ID |
| `updated_at` | TIMESTAMPTZ | Last update timestamp |

The primary key is `(relay_group_id, pipeline_id, outbox_name)`. Offset writes are
monotonic, so a lower acknowledgment cannot rewind a higher stored value.

---

## tide.outbox_cleanup_state

One row per outbox recording the last successful bounded sweep:

| Column | Type | Description |
|---|---|---|
| `outbox_name` | TEXT (PK) | Logical outbox |
| `last_success_at` | TIMESTAMPTZ | Last committed cleanup |
| `last_safe_offset` | BIGINT | Minimum participant checkpoint used |
| `highest_deleted_id` | BIGINT | Highest deleted message ID |
| `last_batch_rows` | BIGINT | Rows deleted by the last batch |
| `cumulative_rows_deleted` | BIGINT | Total rows deleted |
| `last_duration_ms` | DOUBLE PRECISION | Last batch duration |
| `last_partition_action` | TEXT | Partition action or `none` |

The state is updated in the same transaction as deletion or partition removal.

## tide.outbox_storage_config

Singleton physical-layout contract:

| Column | Type | Description |
|---|---|---|
| `layout` | TEXT | `heap`, `id_range`, or `legacy_noncanonical` |
| `partition_span` | BIGINT | ID range for future children |
| `premake` | INTEGER | Future children to maintain |
| `last_maintenance_at` | TIMESTAMPTZ | Last successful provisioning |

## tide.tide_partition_events

Bounded maintenance audit rows for child creation, default-partition drains,
detach/drop decisions, and failures. It contains no payloads.

## tide.outbox_retention_status and tide.relay_pipeline_lag

These views expose the offset-aware retention contract and exact per-pipeline
lag described in the [Outbox API](outbox-api.md). Prefer them over
`outbox_pending` and `consumer_lag` for native operational decisions.
