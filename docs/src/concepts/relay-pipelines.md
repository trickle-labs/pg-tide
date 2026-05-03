# Relay Pipelines

A relay pipeline defines how messages flow between pg_tide and external systems. Pipelines are configured in the database and discovered by the relay binary at runtime.

---

## Two Directions

pg_tide supports two pipeline directions:

### Forward (Outbox → External Sink)

Messages flow from a pg_tide outbox to an external system:

```
PostgreSQL outbox  ──▶  pg-tide relay  ──▶  NATS / Kafka / Redis / Webhook
```

### Reverse (External Source → Inbox)

Messages flow from an external system into a pg_tide inbox:

```
NATS / Kafka / Redis / Webhook  ──▶  pg-tide relay  ──▶  PostgreSQL inbox
```

---

## Configuring Pipelines

Pipelines are stored in the `tide.relay_outbox_config` and `tide.relay_inbox_config` tables. Use the SQL API to manage them:

### Forward Pipeline

```sql
SELECT tide.relay_set_outbox(
  'orders-to-kafka',     -- pipeline name (unique)
  'orders',              -- source outbox
  'kafka',               -- sink type
  jsonb_build_object(    -- sink-specific config
    'brokers', 'localhost:9092',
    'topic', 'order-events'
  ),
  p_batch_size := 100,
  p_enabled := true
);
```

### Reverse Pipeline

```sql
SELECT tide.relay_set_inbox(
  'webhooks-to-inbox',   -- pipeline name
  'incoming-webhooks',   -- target inbox
  jsonb_build_object(    -- source-specific config
    'port', 8080,
    'path', '/webhooks'
  ),
  p_source := 'webhook',
  p_batch_size := 50,
  p_idempotent := true
);
```

---

## Pipeline Lifecycle

```sql
-- Disable (pause processing)
SELECT tide.relay_disable('orders-to-kafka');

-- Re-enable
SELECT tide.relay_enable('orders-to-kafka');

-- Delete permanently
SELECT tide.relay_delete('orders-to-kafka');

-- View current config
SELECT tide.relay_get_config('orders-to-kafka');

-- List all pipelines
SELECT tide.relay_list_configs();
```

---

## Hot Reload

When you create, update, or delete a pipeline, pg_tide fires a `pg_notify('tide_relay_config', ...)` event. The relay binary listens for these notifications and reconfigures itself without restart.

This means you can manage pipeline lifecycle entirely from SQL — no relay restarts, no config file deployments.

---

## Advisory Lock Coordination

Each pipeline is protected by a PostgreSQL advisory lock. When multiple relay instances are running, only one acquires the lock for a given pipeline. This provides:

- **Automatic failover** — if a relay dies, another instance picks up its pipelines
- **No duplicate processing** — only one relay polls each outbox at a time
- **Horizontal scaling** — add more relay instances to handle more pipelines

---

## Supported Backends

| Backend | Forward (Sink) | Reverse (Source) |
|---------|:--------------:|:----------------:|
| NATS | ✓ | ✓ |
| Kafka | ✓ | ✓ |
| Redis Streams | ✓ | ✓ |
| RabbitMQ | ✓ | ✓ |
| SQS | ✓ | ✓ |
| HTTP Webhook | ✓ | ✓ |
| pg_tide Inbox | ✓ | — |
| stdout | ✓ | — |
| stdin | — | ✓ |

See the [Backends section](../relay-guide/backends/index.md) for detailed configuration per backend.
