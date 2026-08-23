# Relay Configuration

The `pg-tide` relay binary is configured through three layers, applied in order of increasing priority:

1. **Default values** — sensible defaults built into the binary
2. **TOML config file** — specified via `--config` or `PG_TIDE_CONFIG`
3. **CLI flags / environment variables** — highest precedence

Pipeline configuration (outbox sources, sink destinations, batch sizes) lives **in PostgreSQL** — not in the TOML file. The relay loads pipeline config from the `tide.relay_outbox_config` and `tide.relay_inbox_config` catalog tables at startup and reloads dynamically via `LISTEN/NOTIFY`.

---

## Quick Start

The only required parameter is the PostgreSQL connection URL:

```bash
pg-tide run --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

For production, use environment variables:

```bash
export PG_TIDE_POSTGRES_URL="postgres://relay:${DB_PASSWORD}@pghost:5432/app"
export PG_TIDE_METRICS_ADDR="0.0.0.0:9090"
export PG_TIDE_LOG_FORMAT="json"
export PG_TIDE_GROUP_ID="prod-relay"

pg-tide run
```

---

## TOML Configuration File

For complex setups, use a TOML file:

```toml
# relay.toml
postgres_url = "${ENV:DATABASE_URL}"
metrics_addr = "0.0.0.0:9090"
log_format = "json"
log_level = "info"
discovery_interval_secs = 30
default_batch_size = 100
relay_group_id = "production"
sink_max_inflight = 1000
```

```bash
pg-tide run --config relay.toml
```

---

## Configuration Reference

| Parameter | CLI Flag | Environment Variable | Default | Description |
|-----------|----------|---------------------|---------|-------------|
| `postgres_url` | `--postgres-url` | `PG_TIDE_POSTGRES_URL` | *(required)* | PostgreSQL connection URL |
| `metrics_addr` | `--metrics-addr` | `PG_TIDE_METRICS_ADDR` | `0.0.0.0:9090` | Prometheus metrics and `/livez`/`/readyz` bind address |
| `log_format` | `--log-format` | `PG_TIDE_LOG_FORMAT` | `text` | Log output format: `text` or `json` |
| `log_level` | `--log-level` | `PG_TIDE_LOG_LEVEL` | `info` | Log verbosity: `error`, `warn`, `info`, `debug`, `trace` |
| `relay_group_id` | `--relay-group-id` | `PG_TIDE_GROUP_ID` | `default` | Relay group identifier for advisory lock namespacing |
| `discovery_interval_secs` | — | — | `30` | Seconds between pipeline discovery polls |
| `default_batch_size` | — | — | `100` | Default messages per batch when not specified per-pipeline |
| `sink_max_inflight` | — | — | `1000` | Maximum in-flight messages before upstream polling pauses. `0` = unlimited |
| `max_owned_pipelines` | `--max-pipelines` | `PG_TIDE_MAX_PIPELINES` | `50` | Maximum pipelines owned by one relay |
| `max_connections` | `--max-connections` | `PG_TIDE_MAX_CONNECTIONS` | `52` | Maximum coordinator pool connections |
| `sweep_interval_hours` | `--sweep-interval-hours` | `PG_TIDE_SWEEP_INTERVAL_HOURS` | `24` | Delivery-receipt cleanup interval |
| — | `--drain-timeout` | `PG_TIDE_DRAIN_TIMEOUT` | `30` | Seconds to wait for in-flight messages to drain on SIGTERM |
| — | `--config` | `PG_TIDE_CONFIG` | — | Path to TOML config file |

---

## Environment Variable Substitution

Connection strings in TOML files support `${ENV:VAR_NAME}` substitution:

```toml
postgres_url = "postgres://${ENV:DB_USER}:${ENV:DB_PASSWORD}@${ENV:DB_HOST}:5432/${ENV:DB_NAME}"
```

This resolves at relay startup time using the process environment. Unknown variables are left as-is (the relay will report a connection error rather than silently using a broken URL).

---

## Relay Group ID

The `relay_group_id` parameter is critical for multi-deployment setups. It controls:

1. **Advisory lock namespacing** — each relay instance acquires a PostgreSQL advisory lock scoped to its group ID + pipeline name. Only one relay per group can own a given pipeline.
2. **Consumer group offset tracking** — progress is tracked per relay group, allowing multiple independent relay deployments to process the same outbox.

**Single deployment (default):**

```bash
pg-tide run --relay-group-id "default"
```

**Multi-region deployment:**

```bash
# US region — processes orders outbox → US NATS
pg-tide run --relay-group-id "us-east" --postgres-url "..."

# EU region — processes same orders outbox → EU NATS
pg-tide run --relay-group-id "eu-west" --postgres-url "..."
```

Each group tracks its own offsets independently — the EU relay won't skip messages just because the US relay already processed them.

---

## Pipeline Configuration (in PostgreSQL)

Pipelines are configured via SQL, not via the relay's TOML/CLI config. The relay discovers pipelines from two catalog tables:

### Forward Pipelines (Outbox → Sink)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-nats',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', '{
      "url": "tls://nats.example:4222",
      "subject": "orders.{event_type}"
    }'::jsonb
  )
);
```

### Enabling / Disabling Pipelines

```sql
-- Disable a pipeline (relay will stop it on next discovery cycle)
SELECT tide.relay_enable('orders-nats', false);

-- Re-enable
SELECT tide.relay_enable('orders-nats', true);
```

Pipeline changes are picked up via:
1. **`LISTEN/NOTIFY`** — immediate reaction to config changes
2. **Periodic polling** — every `discovery_interval_secs` as a fallback

---

## Hot Reload

The relay watches for `NOTIFY` signals on the `tide_relay_config_changed` channel. When you modify a pipeline via `tide.relay_set_outbox_v2()`, the trigger fires a notification and the relay reloads within seconds — no restart required.

If `LISTEN` is interrupted (connection blip), the periodic discovery poll acts as a safety net.

---

## High Availability

Run multiple relay instances with the **same** `relay_group_id`:

```bash
# Instance 1
pg-tide run --relay-group-id "prod" --postgres-url "..."

# Instance 2 (standby — takes over if instance 1 dies)
pg-tide run --relay-group-id "prod" --postgres-url "..."
```

Advisory locks ensure only one instance owns each pipeline at a time. If the owning instance dies, its locks are released and another instance acquires them on the next discovery cycle.

---

## Backpressure

The `sink_max_inflight` parameter controls backpressure behavior:

- When the number of in-flight (unacknowledged) messages reaches this limit, the relay pauses polling from the outbox
- Once the sink acknowledges enough messages to drop below the threshold, polling resumes
- Set to `0` to disable backpressure (not recommended for production)

This prevents the relay from overwhelming a slow sink while still allowing high throughput for fast sinks.

---

## Graceful Shutdown

On `SIGTERM`:

1. The relay stops accepting new pipeline ownership
2. Active pipelines finish their current batch
3. If batches don't complete within `--drain-timeout` seconds (default: 30), the relay exits
4. Unfinished messages will be redelivered when the relay restarts (at-least-once guarantee)

---

## Envelope Encryption (KMS)

pg-tide supports AES-256-GCM envelope encryption via the `kms-local` feature.
Encryption is enabled per-outbox in the `tide.outbox_encryption_config` catalog table.

### KMS Provider Availability

| Provider | Feature flag | Status | Notes |
|---|---|---|---|
| `LocalKeyFile` | `kms-local` | **Fully implemented** (v0.35.0) | AES-256-GCM with key rotation via `key_path_previous`. Ready for production use in non-cloud environments. |

Unsupported providers fail validation or worker construction before the source
polls messages. There is no plaintext fallback.

### LocalKeyFile Configuration

Configure encryption through the outbox catalog API; do not add a
`[pipelines]` section to the process TOML.

Key files must contain exactly 64 hex characters (32 bytes / 256 bits).
Generate a new key: `openssl rand -hex 32 > /etc/pg-tide/kms/current.key`

---

## Example: Production Configuration

```toml
# /etc/pg-tide/relay.toml
postgres_url = "${ENV:DATABASE_URL}"
metrics_addr = "0.0.0.0:9090"
log_format = "json"
log_level = "info"
discovery_interval_secs = 10
default_batch_size = 500
relay_group_id = "production"
sink_max_inflight = 5000
```

```bash
# systemd unit or container entrypoint
PG_TIDE_DRAIN_TIMEOUT=60 pg-tide run --config /etc/pg-tide/relay.toml
```
