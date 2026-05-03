# Configuration Reference

Complete reference for all pg-tide relay configuration options.

---

## TOML Configuration File

```toml
# PostgreSQL connection (required)
postgres_url = "postgres://user:pass@localhost:5432/mydb"

# Metrics + health endpoint
metrics_addr = "0.0.0.0:9090"

# Logging
log_format = "json"    # "text" or "json"
log_level = "info"     # "error", "warn", "info", "debug", "trace"

# Pipeline discovery
discovery_interval_secs = 30

# Default batch size (overridable per-pipeline)
default_batch_size = 100

# Relay group for advisory lock namespacing
relay_group_id = "default"

# Backpressure: max in-flight messages to sink (0 = unlimited)
sink_max_inflight = 1000
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PGTRICKLE_RELAY_POSTGRES_URL` | PostgreSQL connection string |
| `PGTRICKLE_RELAY_METRICS_ADDR` | Metrics endpoint address |
| `PGTRICKLE_RELAY_LOG_FORMAT` | Log format |
| `PGTRICKLE_RELAY_LOG_LEVEL` | Log level |
| `PGTRICKLE_RELAY_GROUP_ID` | Relay group ID |
| `PGTRICKLE_RELAY_CONFIG` | Path to TOML config file |

---

## Connection String Substitution

Use `${ENV:VAR_NAME}` for secret injection:

```toml
postgres_url = "postgres://${ENV:PG_USER}:${ENV:PG_PASSWORD}@${ENV:PG_HOST}:5432/${ENV:PG_DATABASE}"
```

---

## Pipeline Configuration (SQL)

Pipeline-specific configuration is stored in PostgreSQL, not in the TOML file.

### Forward Pipeline Config Keys

```json
{
  "outbox": "outbox-name",
  "sink": "nats|kafka|redis|rabbitmq|sqs|webhook|stdout",
  "batch_size": 100,
  "params": { /* sink-specific */ }
}
```

### Reverse Pipeline Config Keys

```json
{
  "inbox": "inbox-name",
  "source": "nats|kafka|redis|rabbitmq|sqs|webhook|stdin",
  "batch_size": 100,
  "max_retries": 3,
  "idempotent": true,
  "params": { /* source-specific */ }
}
```

---

## Precedence

1. CLI flags (highest)
2. Environment variables
3. TOML config file
4. Built-in defaults (lowest)
