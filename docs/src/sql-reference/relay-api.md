# Relay API

Functions for managing relay pipeline configurations. All live in the `tide` schema.

---

## tide.relay_set_outbox

Configure a forward relay pipeline (outbox → external sink).

```sql
SELECT tide.relay_set_outbox(
  p_name        TEXT,
  p_outbox      TEXT,
  p_sink        TEXT,
  p_config      JSONB    DEFAULT '{}'::jsonb,
  p_batch_size  INT      DEFAULT 100,
  p_enabled     BOOLEAN  DEFAULT true
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Unique pipeline name |
| `p_outbox` | TEXT | (required) | Source outbox name |
| `p_sink` | TEXT | (required) | Sink type: `nats`, `kafka`, `redis`, `rabbitmq`, `sqs`, `webhook`, `stdout` |
| `p_config` | JSONB | `{}` | Sink-specific configuration |
| `p_batch_size` | INT | 100 | Messages per relay batch |
| `p_enabled` | BOOLEAN | true | Whether the pipeline is active |

**Upsert behavior:** If a pipeline with the same name exists, its configuration is updated.

**Example:**

```sql
SELECT tide.relay_set_outbox('orders-to-nats', 'orders', 'nats',
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'orders.{event_type}'
  ),
  p_batch_size := 200
);
```

---

## tide.relay_set_inbox

Configure a reverse relay pipeline (external source → inbox).

```sql
SELECT tide.relay_set_inbox(
  p_name         TEXT,
  p_inbox        TEXT,
  p_config       JSONB    DEFAULT '{}'::jsonb,
  p_batch_size   INT      DEFAULT 100,
  p_source       TEXT     DEFAULT 'stdout',
  p_enabled      BOOLEAN  DEFAULT true,
  p_max_retries  INT      DEFAULT 3,
  p_idempotent   BOOLEAN  DEFAULT true
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Unique pipeline name |
| `p_inbox` | TEXT | (required) | Target inbox name |
| `p_config` | JSONB | `{}` | Source-specific configuration |
| `p_batch_size` | INT | 100 | Messages per batch |
| `p_source` | TEXT | `'stdout'` | Source type: `nats`, `kafka`, `redis`, `rabbitmq`, `sqs`, `webhook`, `stdin` |
| `p_enabled` | BOOLEAN | true | Whether the pipeline is active |
| `p_max_retries` | INT | 3 | Max delivery retries |
| `p_idempotent` | BOOLEAN | true | Use inbox dedup (recommended) |

---

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
