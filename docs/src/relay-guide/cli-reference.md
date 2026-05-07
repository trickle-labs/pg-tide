# CLI Reference

The `pg-tide` binary is both the relay daemon and an operational toolkit. Run it
without a subcommand to start the relay; use a subcommand for diagnostics,
maintenance, and introspection.

---

## Usage

```
pg-tide [OPTIONS] [COMMAND]
```

When `COMMAND` is omitted, the relay daemon starts. All subcommands are
short-lived and exit after completing their task.

---

## Global Options

These flags apply to both daemon mode and all subcommands.

| Flag | Env | Default | Description |
|---|---|---|---|
| `--postgres-url <URL>` | `PG_TIDE_POSTGRES_URL` | — | PostgreSQL connection URL |
| `--metrics-addr <ADDR>` | `PG_TIDE_METRICS_ADDR` | `0.0.0.0:9090` | Prometheus metrics + health endpoint |
| `--log-format <FORMAT>` | `PG_TIDE_LOG_FORMAT` | `text` | `text` or `json` |
| `--log-level <LEVEL>` | `PG_TIDE_LOG_LEVEL` | `info` | `error`, `warn`, `info`, `debug`, `trace` |
| `--relay-group-id <ID>` | `PG_TIDE_RELAY_GROUP_ID` | `default` | Advisory lock namespace; use one value per deployment group |
| `--config <PATH>` | `PG_TIDE_CONFIG` | — | Path to TOML config file; CLI flags override file values |
| `--drain-timeout <SECS>` | `PG_TIDE_DRAIN_TIMEOUT` | `30` | Seconds to wait for in-flight messages to drain on SIGTERM |
| `--max-pipelines <N>` | `PG_TIDE_MAX_PIPELINES` | `50` | Maximum concurrent pipeline workers (each holds one PG connection) |
| `--max-connections <N>` | `PG_TIDE_MAX_CONNECTIONS` | `52` | Coordinator connection pool size |

---

## Daemon Mode

```bash
pg-tide --postgres-url "postgres://relay:secret@db.internal:5432/app"
```

Starts the relay daemon. All pipeline configuration is loaded from PostgreSQL
and hot-reloads on `SIGHUP` without restart. See [Configuration](configuration.md)
for the TOML file format and pipeline schema.

### HTTP Endpoints

| Endpoint | Description |
|---|---|
| `GET /metrics` | Prometheus metrics in text exposition format |
| `GET /health` | `200 OK` when healthy, `503` when unhealthy |

### Signals

| Signal | Behavior |
|---|---|
| `SIGTERM` / `SIGINT` | Graceful shutdown: drain in-flight messages, release advisory locks, exit |
| `SIGHUP` | Hot-reload pipeline configuration from PostgreSQL without downtime |

---

## Subcommands

### `doctor`

Validates PostgreSQL connectivity, schema version, and catalog health.

```bash
pg-tide doctor [--postgres-url <URL>]
```

**Checks performed:**

- TCP connectivity and TLS handshake to PostgreSQL
- Existence of the `tide` schema
- Presence of all required catalog tables
- `relay_consumer_offsets.last_change_id` column (v0.12.0+ migration marker)
- Presence of `tide.outbox_truncate_delivered()` function (v0.15.0+)
- Count of configured forward and reverse pipelines

**Exit codes:** `0` = all checks passed, `1` = one or more failures.

**Example output:**

```
pg-tide doctor v0.16.0
Connecting to PostgreSQL...
  [OK] Connected to PostgreSQL
  [OK] Schema 'tide' exists
  [OK] Table tide.tide_outbox_config
  [OK] Table tide.relay_outbox_config
  [OK] relay_consumer_offsets.last_change_id column present
  [OK] tide.outbox_truncate_delivered() present (v0.15.0+)
  [INFO] 3 forward pipeline(s), 1 reverse pipeline(s) configured

pg-tide doctor: all checks passed.
```

**Typical use:** health check in CI, post-deploy validation, Kubernetes
`readinessProbe` via a Job.

---

### `validate-config`

Dry-runs source and sink factory construction for a named pipeline without
processing any messages.

```bash
pg-tide validate-config --pipeline <NAME> [--postgres-url <URL>]
```

**What it does:**

1. Loads the pipeline config from `tide.relay_outbox_config` or
   `tide.relay_inbox_config`
2. Resolves all `${ENV:VAR}` secret placeholders
3. Constructs the source implementation (e.g., outbox poller, Kafka consumer)
4. Constructs the sink implementation (e.g., Kafka producer, HTTP webhook)
5. Reports success or the first construction failure

No messages are read or published. Exit `0` = config is valid, `1` = failure.

**Example:**

```bash
pg-tide validate-config \
  --pipeline orders-kafka \
  --postgres-url "$DATABASE_URL"
```

```
pg-tide validate-config — pipeline: orders-kafka
  [OK] Secrets resolved
  [OK] Source 'outbox:orders' instantiated
  [OK] Sink 'kafka:orders.events' instantiated

validate-config: pipeline 'orders-kafka' configuration is valid.
```

**Typical use:** pre-flight check before enabling a new pipeline; CI step
after updating sink credentials.

---

### `status`

Prints a summary table of all configured relay pipelines.

```bash
pg-tide status [--postgres-url <URL>]
```

**Columns:**

| Column | Description |
|---|---|
| `PIPELINE` | Pipeline name |
| `DIRECTION` | `forward` (outbox → sink) or `reverse` (source → inbox) |
| `ENABLED` | Whether the pipeline is enabled in the catalog |
| `LAST_OFFSET` | Last committed change ID (0 if never consumed) |
| `CONSUMER_LAG` | Unconsumed outbox messages (forward pipelines only) |

**Example:**

```
PIPELINE                       DIRECTION  ENABLED  LAST_OFFSET    CONSUMER_LAG
--------------------------------------------------------------------------------
orders-kafka                   forward    yes      1842731        0
payments-kafka                 forward    yes      998201         3
webhooks-incoming              reverse    yes      0              0
audit-log                      forward    no       0              0

4 pipeline(s) configured.
```

> Consumer lag is queried from the outbox table at snapshot time; it does not
> reflect in-flight messages being processed by a running relay.

---

### `sweep`

Deletes consumed outbox messages that are past their retention window.

```bash
pg-tide sweep [--outbox <NAME>] [--postgres-url <URL>]
```

Calls `tide.outbox_truncate_delivered()` for each outbox. When `--outbox` is
omitted, all outboxes are swept. Run this on a schedule to prevent unbounded
growth of the `tide_outbox_messages` table.

**Example:**

```bash
# Sweep all outboxes
pg-tide sweep --postgres-url "$DATABASE_URL"

# Sweep a single outbox
pg-tide sweep --outbox orders --postgres-url "$DATABASE_URL"
```

```
pg-tide sweep v0.16.0
  [OK] Swept outbox 'orders': 12408 rows deleted
  [OK] Swept outbox 'payments': 4891 rows deleted

pg-tide sweep: 17299 total row(s) deleted from 2 outbox(es).
```

**Typical use:** cron job or Kubernetes CronJob running every hour.

```bash
# Kubernetes CronJob
schedule: "0 * * * *"
command: ["pg-tide", "sweep", "--postgres-url", "$(DATABASE_URL)"]
```

---

### `replay`

Replay workbench for inspecting, debugging, and recovering from delivery
failures. All replay subcommands are read-only or operate only on DLQ metadata —
they never advance consumer offsets.

#### `replay preview`

Print outbox messages in an ID range as JSONL without consuming them.

```bash
pg-tide replay preview \
  --outbox <NAME> \
  [--from-id <ID>] \
  [--to-id <ID>] \
  [--limit <N>] \
  [--postgres-url <URL>]
```

| Flag | Default | Description |
|---|---|---|
| `--outbox` | required | Outbox name to preview |
| `--from-id` | `0` | Start of ID range (inclusive) |
| `--to-id` | `i64::MAX` | End of ID range (inclusive) |
| `--limit` | `100` | Maximum rows to return |

Output is JSONL on stdout; progress is printed to stderr.

```bash
pg-tide replay preview --outbox orders --from-id 1840000 --limit 5
```

```json
{"id":1840001,"outbox_name":"orders","payload":{"order_id":42},"headers":{},"created_at":"2026-05-07T10:00:01Z","consumed":true}
{"id":1840002,"outbox_name":"orders","payload":{"order_id":43},"headers":{},"created_at":"2026-05-07T10:00:02Z","consumed":false}
```

#### `replay dry-run`

Evaluate a pipeline's transforms against a sample of outbox messages and print
the resulting envelopes to stdout — without publishing anything.

```bash
pg-tide replay dry-run \
  --pipeline <NAME> \
  [--from-id <ID>] \
  [--to-id <ID>] \
  [--limit <N>] \
  [--postgres-url <URL>]
```

Useful for verifying JMESPath transform expressions and wire format output
before enabling a pipeline.

```bash
pg-tide replay dry-run --pipeline orders-kafka --limit 3
```

```
Dry-run transform evaluation for pipeline 'orders-kafka' (3 message(s)):
{"outbox_id":1840001,"event_id":"uuid-...","op":"c","payload":{...}}
{"outbox_id":1840002,"event_id":"uuid-...","op":"u","payload":{...}}
  [SKIP] id=1840003 (tombstone or filtered)
```

#### `replay dlq-resolve`

Mark a DLQ entry as resolved (closed without requeue).

```bash
pg-tide replay dlq-resolve \
  --pipeline <NAME> \
  --dedup-key <KEY> \
  [--postgres-url <URL>]
```

Sets `resolved = true` on the DLQ row. The message will not be retried.
Use when the failure is expected or the downstream system has been manually
updated.

#### `replay dlq-requeue`

Requeue a DLQ entry for another relay attempt.

```bash
pg-tide replay dlq-requeue \
  --pipeline <NAME> \
  --dedup-key <KEY> \
  [--postgres-url <URL>]
```

Marks the current DLQ entry resolved and inserts a fresh pending entry with
`attempt_count = 0`. The running relay will pick it up on the next cycle.

---

### `asyncapi export`

Generate an AsyncAPI 3.0 document from relay catalog metadata.

```bash
pg-tide asyncapi export \
  [--format yaml|json] \
  [--output <PATH>] \
  [--postgres-url <URL>]
```

| Flag | Default | Description |
|---|---|---|
| `--format` | `yaml` | Output format: `yaml` or `json` |
| `--output` | stdout | File path to write the document; omit to print to stdout |

Reads all configured outbox and inbox pipelines from PostgreSQL and emits an
AsyncAPI 3.0 document describing each pipeline as a named channel, operation,
and message schema. Useful for API documentation, consumer contract testing
with [Microcks](../integration/microcks.md), and downstream code generation.

```bash
pg-tide asyncapi export \
  --format yaml \
  --output relay-asyncapi.yaml \
  --postgres-url "$DATABASE_URL"
```

See [Microcks Integration](../integration/microcks.md) for a complete guide on
using the exported spec for consumer contract testing.

---

## Daemon Startup Examples

### Minimal

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

### Production

```bash
pg-tide \
  --postgres-url "postgres://relay:secret@db.internal:5432/app" \
  --log-format json \
  --log-level info \
  --relay-group-id production \
  --metrics-addr 0.0.0.0:9090 \
  --drain-timeout 60
```

### From config file

```bash
pg-tide --config /etc/pg-tide/relay.toml
```

### Docker

```bash
docker run \
  -e PG_TIDE_POSTGRES_URL="postgres://..." \
  -p 9090:9090 \
  ghcr.io/trickle-labs/pg-tide:latest
```
