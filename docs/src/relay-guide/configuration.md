# Relay Configuration

The `pg-tide` relay binary reads its process-level configuration from CLI flags, environment variables, or a TOML config file. Pipeline definitions (which outbox connects to which sink) are stored in PostgreSQL — not in the config file.

---

## Configuration Sources (Priority Order)

1. **CLI flags** — highest priority
2. **Environment variables** — override TOML
3. **TOML config file** — base configuration

---

## TOML Config File

```toml
# pg-tide relay configuration
postgres_url = "postgres://user:pass@localhost:5432/mydb"
metrics_addr = "0.0.0.0:9090"
log_format = "json"
log_level = "info"
discovery_interval_secs = 30
default_batch_size = 100
relay_group_id = "production"
sink_max_inflight = 1000
```

Pass the config file path via:

```bash
pg-tide --config /etc/pg-tide/relay.toml
```

---

## Configuration Options

| Option | CLI Flag | Env Variable | Default | Description |
|--------|----------|-------------|---------|-------------|
| `postgres_url` | `--postgres-url` | `PGTRICKLE_RELAY_POSTGRES_URL` | (required) | PostgreSQL connection string |
| `metrics_addr` | `--metrics-addr` | `PGTRICKLE_RELAY_METRICS_ADDR` | `0.0.0.0:9090` | Metrics + health endpoint |
| `log_format` | `--log-format` | `PGTRICKLE_RELAY_LOG_FORMAT` | `text` | `text` or `json` |
| `log_level` | `--log-level` | `PGTRICKLE_RELAY_LOG_LEVEL` | `info` | `error`, `warn`, `info`, `debug`, `trace` |
| `relay_group_id` | `--relay-group-id` | `PGTRICKLE_RELAY_GROUP_ID` | `default` | Unique ID for advisory lock namespacing |
| `discovery_interval_secs` | — | — | `30` | Seconds between pipeline discovery polls |
| `default_batch_size` | — | — | `100` | Default batch size per pipeline |
| `sink_max_inflight` | — | — | `1000` | Max in-flight messages before pausing (0 = unlimited) |

---

## Environment Variable Substitution

Connection strings support `${ENV:VAR_NAME}` placeholders:

```toml
postgres_url = "postgres://${ENV:PG_USER}:${ENV:PG_PASSWORD}@${ENV:PG_HOST}:5432/mydb"
```

Only process environment variables are read — no shell expansion or eval.

---

## Pipeline Configuration

Pipeline definitions are **not** in the TOML file. They live in PostgreSQL:

```sql
-- Forward pipeline
SELECT tide.relay_set_outbox('my-pipeline', 'my-outbox', 'nats',
  '{"url": "nats://nats:4222", "subject": "events"}'::jsonb
);

-- Reverse pipeline
SELECT tide.relay_set_inbox('webhooks-in', 'incoming',
  '{"port": 8080}'::jsonb,
  p_source := 'webhook'
);
```

This allows hot-reload: change pipeline config in the database, and the relay picks it up via LISTEN/NOTIFY without restart.

---

## Relay Group ID

The `relay_group_id` namespaces advisory locks. Multiple relay instances with the **same** group ID compete for pipeline ownership (HA failover). Instances with **different** group IDs can own the same pipeline simultaneously (use with caution).

```bash
# Production HA pair — same group, automatic failover
pg-tide --relay-group-id production --postgres-url ...
pg-tide --relay-group-id production --postgres-url ...

# Separate staging relay — different group
pg-tide --relay-group-id staging --postgres-url ...
```
