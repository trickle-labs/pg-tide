# Relay API

Functions for managing relay pipeline configurations. All live in the `tide` schema.

---

## tide.relay_set_outbox_v2

Configure a forward relay pipeline (outbox → external sink). Takes a single
JSONB config object. The positional `relay_set_outbox(...)` form was removed in
v0.36.0.

```sql
SELECT tide.relay_set_outbox_v2(config JSONB);
```

| Config key | Type | Default | Description |
|------------|------|---------|-------------|
| `name` | TEXT | (required) | Unique pipeline name |
| `outbox` | TEXT | (required) | Source outbox name (must already exist) |
| `sink_type` | TEXT | (required) | Sink type: `inbox`, `nats`, `kafka`, `webhook`, `stdout`, or `file` |
| `config` | JSONB | `{}` | Sink-specific configuration |
| `wire_format` | TEXT | `native` | `native` or `cloudevents` |
| `batch_size` | INT | 100 | Messages per relay batch |
| `enabled` | BOOLEAN | true | Whether the pipeline is active |

The relay always polls the canonical `tide.tide_outbox_messages` table.

**Upsert behavior:** If a pipeline with the same name exists, its configuration is updated.

**Example:**

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-nats',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats.example:4222',
      'subject_template', 'orders.{event_type}'
    ),
    'batch_size', 200
  )
);
```

## tide.relay_enable

Enable a previously disabled pipeline.

```sql
SELECT tide.relay_enable(p_name TEXT);
```

Fires `pg_notify('tide_relay_config', name)` to trigger hot-reload in the relay.

---

## tide.relay_disable

Disable a pipeline (stops processing without deleting config).

```sql
SELECT tide.relay_disable(p_name TEXT);
```

---

## tide.relay_delete

Permanently delete a pipeline configuration.

```sql
SELECT tide.relay_delete(p_name TEXT);
```

---

## tide.relay_get_config

Retrieve the full configuration for a pipeline.

```sql
SELECT tide.relay_get_config(p_name TEXT) → JSONB
```

**Returns** the stored config JSONB for the named pipeline.

---

## tide.relay_list_configs

List all configured relay pipelines.

```sql
SELECT tide.relay_list_configs() → JSONB
```

**Returns** a JSON array of all pipelines with their direction and enabled status:

```json
[
  {"name": "orders-to-nats", "direction": "outbox", "enabled": true},
  {"name": "webhooks-in", "direction": "inbox", "enabled": true}
]
```
