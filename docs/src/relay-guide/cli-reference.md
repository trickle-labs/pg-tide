# CLI Reference

The `pg-tide` binary accepts the following command-line arguments.

---

## Usage

```
pg-tide [OPTIONS]
```

## Options

```
--postgres-url <URL>
    PostgreSQL connection URL (required).
    Example: postgres://user:pass@localhost:5432/mydb
    Env: PGTRICKLE_RELAY_POSTGRES_URL

--metrics-addr <ADDR>
    Prometheus metrics + health endpoint address.
    Default: 0.0.0.0:9090
    Env: PGTRICKLE_RELAY_METRICS_ADDR

--log-format <FORMAT>
    Log output format: text or json.
    Default: text
    Env: PGTRICKLE_RELAY_LOG_FORMAT

--log-level <LEVEL>
    Log verbosity: error, warn, info, debug, trace.
    Default: info
    Env: PGTRICKLE_RELAY_LOG_LEVEL

--relay-group-id <ID>
    Relay group ID for advisory lock namespacing.
    Use a unique value per deployment group.
    Default: default
    Env: PGTRICKLE_RELAY_GROUP_ID

--config <PATH>
    Path to TOML configuration file (optional).
    CLI flags take precedence over file values.
    Env: PGTRICKLE_RELAY_CONFIG

--version
    Print version and exit.

--help
    Print help information.
```

---

## Examples

### Minimal startup

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

### Production with JSON logging

```bash
pg-tide \
  --postgres-url "postgres://relay:secret@db.internal:5432/app" \
  --log-format json \
  --log-level info \
  --relay-group-id production \
  --metrics-addr 0.0.0.0:9090
```

### Using a config file

```bash
pg-tide --config /etc/pg-tide/relay.toml
```

### Docker

```bash
docker run -e PGTRICKLE_RELAY_POSTGRES_URL="postgres://..." \
  ghcr.io/trickle-labs/pg-tide:latest
```

---

## Endpoints

The relay exposes HTTP endpoints on the metrics address:

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics in text format |
| `GET /health` | Health check (200 = healthy, 503 = unhealthy) |

---

## Signals

| Signal | Behavior |
|--------|----------|
| `SIGTERM` | Graceful shutdown: drain in-flight messages, release locks, exit |
| `SIGINT` (Ctrl+C) | Same as SIGTERM |
