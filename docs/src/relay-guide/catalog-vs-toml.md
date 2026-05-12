# Catalog vs. TOML: Configuration Hierarchy

pg-tide has two places where configuration lives.  This page explains which
is the **single source of truth** and how to use each correctly.

---

## The Rule: Catalog Is Primary, TOML Is Process Config

| What | Where | Source of truth |
|------|-------|-----------------|
| Which pipelines exist | `tide.relay_outbox_config` / `tide.relay_inbox_config` | **Catalog (SQL)** |
| Pipeline direction (forward / reverse) | Catalog | **Catalog (SQL)** |
| Sink type, sink config, batch size | Catalog `config` JSONB column | **Catalog (SQL)** |
| Wire format, DLQ settings | Catalog `config` JSONB column | **Catalog (SQL)** |
| PostgreSQL credentials | `--postgres-url-file` / `PG_TIDE_POSTGRES_URL` | TOML / env var |
| Relay group identity | `relay_group_id` TOML key | TOML / env var |
| Resource limits (`max_owned_pipelines`, `max_connections`) | TOML / CLI flags | TOML / CLI |
| Logging format and level | TOML / CLI flags | TOML / CLI |
| Metrics address | TOML / CLI flags | TOML / CLI |

---

## Managing Pipelines via SQL (Recommended)

All pipeline configuration should be managed through the `tide` schema
SQL functions:

```sql
-- Create or update a forward pipeline (outbox → NATS):
SELECT tide.relay_set_outbox_v2('{
  "name":      "orders-to-nats",
  "outbox":    "orders",
  "sink_type": "nats",
  "config": {
    "url":     "nats://nats.svc.cluster.local:4222",
    "subject": "orders.{event_type}"
  },
  "batch_size": 50,
  "enabled":    true
}'::jsonb);

-- Disable a pipeline without deleting it:
SELECT tide.relay_disable('orders-to-nats');

-- Re-enable:
SELECT tide.relay_enable('orders-to-nats');

-- List all configured pipelines:
SELECT name, direction, enabled, config->>'sink_type' AS sink
FROM   tide.relay_outbox_config
UNION ALL
SELECT name, 'reverse', enabled, config->>'source_type'
FROM   tide.relay_inbox_config;
```

Changes take effect within one second thanks to the `LISTEN/NOTIFY` hot-reload
introduced in v0.18.0.  There is no need to restart the relay.

---

## TOML File — Process Configuration Only

The TOML file (default: `/etc/pg-tide/pg-tide.toml`) configures the relay
**process**, not the pipelines.  It should contain only:

```toml
postgres_url        = "..."   # or use --postgres-url-file
relay_group_id      = "prod"
max_owned_pipelines = 50
max_connections     = 100
discovery_interval_secs = 30
default_batch_size  = 100
metrics_addr        = "0.0.0.0:9090"
log_level           = "info"
log_format          = "json"
drain_timeout_secs  = 30
```

A fully commented example is baked into the Docker image at
`/etc/pg-tide/pg-tide.example.toml`.  Copy it as a starting point:

```bash
docker cp pg-tide:/etc/pg-tide/pg-tide.example.toml ./pg-tide.toml
```

---

## Startup Warning: TOML-Only Pipelines

If the TOML file configures a pipeline that is **not** present in the
catalog (e.g. via a legacy `[pipelines.*]` section), the relay emits a
`WARN`-level log entry at startup:

```
WARN pipeline "orders-to-nats" defined in TOML but not found in catalog — ignoring
```

The expected resolution is to create the pipeline via SQL using
`tide.relay_set_outbox_v2()` or `tide.relay_set_inbox_v2()`, then remove
the TOML definition.

---

## Secret Interpolation

Sensitive values (passwords, API keys) should **not** be stored in the
catalog JSONB config in plain text.  Instead, use the `${ENV:VAR_NAME}` or
`${FILE:/path/to/secret}` interpolation syntax in the config JSON:

```sql
SELECT tide.relay_set_outbox_v2('{
  "name":      "orders-to-kafka",
  "outbox":    "orders",
  "sink_type": "kafka",
  "config": {
    "brokers":  "kafka.svc:9092",
    "topic":    "orders",
    "sasl_password": "${ENV:KAFKA_SASL_PASSWORD}"
  }
}'::jsonb);
```

The relay resolves `${ENV:...}` tokens at runtime, keeping the secret out of
the database entirely.

---

## See Also

- [Configuration reference](configuration.md)
- [SQL API reference](../sql-reference/)
- [Operations runbooks](../operations/)
