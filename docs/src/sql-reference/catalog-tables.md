# Catalog Tables

pg_tide stores all state in relational tables within the `tide` schema. This page documents the underlying tables that power the SQL API.

---

## tide.tide_outbox_config

One row per named outbox.

| Column | Type | Description |
|--------|------|-------------|
| `outbox_name` | TEXT (PK) | Unique outbox identifier |
| `retention_hours` | INT | Hours to retain consumed messages |
| `inline_threshold` | INT | Backpressure threshold |
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
| `consumed_at` | TIMESTAMPTZ | When relay delivered it (NULL = pending) |
| `consumer_group` | TEXT | Which group consumed it |

**Indexes:**
- `idx_tide_outbox_messages_pending` — partial index on `(outbox_name, id) WHERE consumed_at IS NULL`

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
| `last_offset` | TEXT | Last processed offset |
| `updated_at` | TIMESTAMPTZ | Last update timestamp |
