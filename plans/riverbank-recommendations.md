# pg-tide — Improvements Identified from riverbank Planning

> **Date:** 2026-05-05
> **Source:** Analysis of the [riverbank roadmap](../ROADMAP.md) and strategy
> documents during riverbank planning. These items were initially scoped to
> riverbank but belong in pg-tide: they are about transport reliability,
> content-based routing, relay protocol compliance, or relay pipeline
> observability — none of which require LLM calls or access to business logic
> in the compilation layer.
>
> **Principle:** pg-tide owns everything that is about bridging the
> pg_trickle outbox/inbox with external systems: transport, routing, buffering,
> reliability, protocol compliance, and relay health. riverbank owns anything
> that requires orchestrating LLMs, managing a compilation lifecycle, or
> executing domain-specific business logic.

---

## 1. Content-based payload routing on outbox relays

### Background

riverbank's §10.5 agent memory bus publishes typed semantic events through the
pg_trickle outbox:

```
entity.updated
policy.changed
policy.contradiction.detected
summary.invalidated
source.needs_review
answer_package.changed
```

All of these flow through the same outbox stream table
(`entity_updates`). Downstream agents and systems want to subscribe selectively:
a compliance dashboard cares about `policy.changed` and
`policy.contradiction.detected`; a documentation site cares about
`entity.updated` and `summary.invalidated`. Routing today requires either
splitting into separate stream tables (wasteful, fragile) or letting every
consumer receive every event type and filter client-side (inefficient, noisy).

pg-tide currently routes by stream table name. It has no concept of routing
based on the content of the JSONB payload it is forwarding. This is a gap:
it forces application-level routing logic into places that should stay in the
transport layer.

### Proposed addition

A `payload_filter` field in `tide.relay_outbox_config` that accepts a JSONB
path expression (PostgreSQL `@>` containment semantics) to match before
forwarding:

```sql
-- Forward only policy change events to the compliance team's NATS subject.
INSERT INTO tide.relay_outbox_config (pipeline_name, enabled, config)
VALUES ('policy-events', true,
    '{"stream_table":     "entity_updates",
      "sink_type":        "nats",
      "nats_url":         "${env:NATS_URL}",
      "subject_template": "riverbank.policy.{event_type}",
      "payload_filter":   {"event_type": "policy.changed"}
    }'::jsonb);

-- Forward watchpoint-fired events to a webhook target.
INSERT INTO tide.relay_outbox_config (pipeline_name, enabled, config)
VALUES ('watchpoint-alerts', true,
    '{"stream_table":     "entity_updates",
      "sink_type":        "webhook",
      "webhook_url":      "${env:ALERT_WEBHOOK_URL}",
      "payload_filter":   {"pgc:eventClass": "pgc:WatchpointFired"}
    }'::jsonb);
```

The filter is evaluated against the outbox row's payload column (JSONB) before
the forward attempt. Rows that do not match are skipped without acknowledgement
cost. Multiple pipelines on the same stream table each apply their own filter
independently — one stream table, N typed relay channels.

`subject_template` gains a `{event_type}` interpolation variable populated from
the payload field of that name, enabling per-event-type NATS subjects without
separate pipeline configs.

### riverbank impact

The agent memory bus (§10.5) is fully declarative: riverbank writes typed
events to the outbox; SQL config in `tide.relay_outbox_config` routes them.
riverbank does not implement any fan-out, content inspection, or consumer
management in Python. The `source.needs_review` event type, for example,
routes directly to Label Studio's webhook without any Python intermediary.

### Acceptance criteria

- A relay pipeline with `payload_filter` only forwards rows whose payload
  contains the specified fields and values; non-matching rows are not
  forwarded and do not appear in the DLQ (see item 3).
- A single stream table with three pipelines, each with a different
  `payload_filter`, delivers each event to exactly one downstream target.
- `payload_filter: {}` (empty object) matches all rows — backward-compatible
  default behaviour.
- Hot-reload applies to `payload_filter` changes without restarting the relay.

---

## 2. Circuit breakers and configurable retry policies for relay pipelines

### Background

riverbank v0.6.0 plans circuit breakers (`aiobreaker`) for LLM API calls:
protect against runaway costs when an upstream provider is misbehaving. The
same concern applies to pg-tide relay pipelines, and the fix belongs in
pg-tide, not in riverbank.

When a forward relay target (e.g., a webhook endpoint or NATS broker) becomes
unavailable, pg-tide's current behaviour under persistent failure is
unspecified in the plans — it is likely to retry and queue internally, but
without a circuit breaker it risks:

- Hammering a degraded endpoint and worsening its recovery
- Holding advisory locks while blocked on a remote call
- Growing unbounded internal retry queues

These are transport concerns. They belong in pg-tide's relay engine, not in
any application layer.

### Proposed addition

Per-pipeline retry and circuit breaker configuration in `tide.relay_outbox_config`
and `tide.relay_inbox_config`:

```sql
INSERT INTO tide.relay_outbox_config (pipeline_name, enabled, config)
VALUES ('knowledge-events', true,
    '{"stream_table":            "entity_updates",
      "sink_type":               "nats",
      "nats_url":                "${env:NATS_URL}",
      "subject_template":        "riverbank.{event_type}",
      "retry": {
          "max_attempts":        5,
          "initial_backoff_ms":  500,
          "max_backoff_ms":      30000,
          "backoff_multiplier":  2.0,
          "jitter":              true
      },
      "circuit_breaker": {
          "failure_threshold":   5,
          "probe_interval_s":    60,
          "half_open_probes":    2
      }
    }'::jsonb);
```

Circuit breaker states:
- **Closed** (normal): events forwarded as they arrive; failure count tracked.
- **Open** (tripped): forward attempts skipped; events written to the DLQ (item 3). Re-entry attempted after `probe_interval_s`.
- **Half-open** (probing): `half_open_probes` trial sends; if all succeed, circuit closes; if any fail, circuit reopens.

The circuit breaker state per pipeline is observable:

```sql
SELECT * FROM tide.relay_circuit_breaker_status;
-- pipeline_name, state, failure_count, last_failure_at, next_probe_at
```

### riverbank impact

riverbank's v0.6.0 rate-limiting and circuit-breaker deliverable is reduced to
LLM provider protection only (`aiobreaker` for OpenAI / Anthropic / Ollama
API calls). Relay pipeline resilience is fully handled by pg-tide configuration,
with no Python code required.

`riverbank health` gains a check that calls
`SELECT * FROM tide.relay_circuit_breaker_status` and surfaces any open
circuits alongside the existing `pgtrickle.preflight()` and
`pg_ripple.pg_tide_available()` checks.

### Acceptance criteria

- A relay pipeline in `open` state skips forward attempts and writes the
  skipped events to the DLQ; this is visible in `relay_circuit_breaker_status`.
- After `probe_interval_s` seconds, the pipeline enters `half_open` and
  attempts `half_open_probes` trial sends.
- Successful probes close the circuit; the DLQ events are flushed according
  to the DLQ flush policy (item 3).
- `retry` config honours `jitter`: successive retries do not all land at the
  same moment on recovery.
- All circuit breaker state transitions appear as Prometheus metric label
  changes (item 4).

---

## 3. Dead letter queue for undeliverable outbox events

### Background

When a forward relay fails permanently — the webhook endpoint has been
decommissioned, the NATS cluster is rebuilt with a different subject namespace,
the Kafka topic was deleted — events should not be silently dropped. They should
be preserved in a dead letter queue that operators can inspect and replay.

pg-tide already has an Object Storage backend (S3, GCS, Azure Blob). Using it
as the DLQ storage is natural: failed events are written as newline-delimited
JSON files, partitioned by pipeline name and timestamp, queryable with Athena /
BigQuery / DuckDB.

### Proposed addition

A DLQ configuration block in `tide.relay_outbox_config`:

```sql
INSERT INTO tide.relay_outbox_config (pipeline_name, enabled, config)
VALUES ('knowledge-events', true,
    '{"stream_table":   "entity_updates",
      "sink_type":      "nats",
      "nats_url":       "${env:NATS_URL}",
      "subject_template": "riverbank.{event_type}",
      "retry":          {"max_attempts": 5, "initial_backoff_ms": 500},
      "circuit_breaker": {"failure_threshold": 5, "probe_interval_s": 60},
      "dlq": {
          "sink_type":    "s3",
          "s3_bucket":    "${env:DLQ_BUCKET}",
          "s3_prefix":    "pg-tide-dlq/{pipeline_name}/{date}/",
          "flush_on_circuit_close": true
      }
    }'::jsonb);
```

DLQ behaviour:
- Events are written to the DLQ after `retry.max_attempts` failures, or
  immediately when the circuit breaker is open.
- Each DLQ record is a JSONB object: `{pipeline_name, event_id, payload,
  failed_at, failure_reason, attempt_count}`.
- `flush_on_circuit_close: true` replays DLQ events back through the relay
  when the circuit closes (half-open probes succeed). Replay is rate-limited
  to avoid thundering herd.
- A SQL view makes DLQ depth observable without accessing Object Storage:

```sql
SELECT * FROM tide.relay_dlq_summary;
-- pipeline_name, dlq_depth, oldest_failed_at, newest_failed_at
```

An explicit replay command replays events from the DLQ for a named pipeline:

```sql
SELECT tide.replay_dlq(pipeline_name => 'knowledge-events', limit => 100);
```

DLQ can also be configured as a PostgreSQL table instead of Object Storage —
useful for smaller deployments or local development:

```sql
"dlq": {"sink_type": "pg_table", "table_name": "tide_dlq"}
```

### riverbank impact

riverbank does not need to implement any "catch failed events and re-enqueue"
logic. The DLQ is a transport concern handled entirely by pg-tide. `riverbank
health` includes `DLQ depth` from `tide.relay_dlq_summary` in its health
output.

### Acceptance criteria

- After `max_attempts` failures, the event appears in `relay_dlq_summary`
  with correct `failure_reason`.
- `tide.replay_dlq('pipeline_name')` replays events in arrival order,
  respecting the pipeline's retry config.
- DLQ depth is exposed as a Prometheus metric (item 4).
- `flush_on_circuit_close: true` replays DLQ events after a successful
  half-open probe without operator intervention.

---

## 4. Per-pipeline Prometheus metrics

### Background

riverbank v0.6.0 plans a `/metrics` endpoint exposing compilation-specific
metrics (`riverbank_runs_total`, `riverbank_llm_cost_usd_total`, etc.).
pg-tide already exposes port 9090 for metrics, but the plans do not specify
what relay-level metrics it currently exposes.

Relay pipeline health metrics — throughput, error rate, backlog depth, circuit
breaker state — belong in pg-tide's Prometheus exporter, not in riverbank's.
These metrics are useful to any pg-tide operator, not just riverbank users.
A Perses (or Grafana) dashboard built on top of them is deployable standalone.

### Proposed metrics

All metrics are labelled with `pipeline_name` and `pipeline_direction`
(`forward` or `reverse`):

| Metric name | Type | Description |
|---|---|---|
| `pgtide_relay_messages_forwarded_total` | counter | Total messages successfully forwarded per pipeline |
| `pgtide_relay_messages_failed_total` | counter | Total forward attempts that ultimately failed (after retries) |
| `pgtide_relay_messages_dlq_total` | counter | Total messages written to the DLQ |
| `pgtide_relay_backlog_depth` | gauge | Current number of outbox rows not yet acknowledged by the relay |
| `pgtide_relay_forward_latency_seconds` | histogram | Time from outbox row insertion to successful forward acknowledgement |
| `pgtide_relay_circuit_breaker_state` | gauge | `0` = closed, `1` = half-open, `2` = open |
| `pgtide_relay_retry_attempts_total` | counter | Total retry attempts per pipeline (labelled with `attempt_number`) |
| `pgtide_relay_inbox_messages_received_total` | counter | Total messages received on reverse pipelines |
| `pgtide_relay_inbox_lag_seconds` | gauge | Time between source event emission and inbox row insertion |

### Perses dashboard

A Perses (or Grafana-compatible) dashboard definition ships in
`pg-tide/dashboards/relay-health.json`. Panels:

- **Throughput**: `pgtide_relay_messages_forwarded_total` rate per pipeline,
  stacked by pipeline name
- **Error rate**: ratio of `messages_failed_total` to `messages_forwarded_total`
- **DLQ depth**: `pgtide_relay_messages_dlq_total` with alert threshold
- **Backlog**: `pgtide_relay_backlog_depth` per pipeline
- **Circuit breaker**: `pgtide_relay_circuit_breaker_state` with colour
  encoding (green/amber/red)
- **Forward latency**: p50/p95/p99 from `pgtide_relay_forward_latency_seconds`

### riverbank impact

riverbank's Perses dashboards (`riverbank/perses/`) include relay health
panels by importing the pg-tide Perses dashboard as a sub-dashboard, rather
than reimplementing the relay metrics. riverbank's own panels cover:
`riverbank_runs_total`, `riverbank_llm_cost_usd_total`, `riverbank_shacl_score`,
`riverbank_review_queue_depth`.

### Acceptance criteria

- All metrics listed above are present on the `9090/metrics` endpoint with
  correct `pipeline_name` labels when at least one pipeline is configured.
- `pgtide_relay_circuit_breaker_state` transitions from `0` to `2` when the
  circuit breaker opens, and back to `0` when it closes.
- The Perses dashboard JSON is valid and importable without modification on
  Perses ≥ 0.44.

---

## 5. Singer target: STATE and SCHEMA message handling

### Background

riverbank v0.4.0 plans Singer tap support via `tap-github | pg-tide --target
singer` — piping any Singer tap directly into the pg_trickle inbox, bypassing
the Python connector. This is already supported. But the Singer specification
includes three message types, not one:

- **RECORD** — a data row (currently handled: written to inbox)
- **STATE** — a checkpoint bookmark the target must persist for restart recovery
- **SCHEMA** — a schema declaration (column names/types) the target should respect

pg-tide's current Singer target mode handles RECORD messages. STATE and SCHEMA
are not addressed in the plans. Without STATE persistence, restarting pg-tide
causes the tap to re-emit all records from the beginning (full re-ingest rather
than incremental). Without SCHEMA handling, schema drift in a tap is invisible
until downstream failures occur.

These are Singer protocol responsibilities. They belong in pg-tide, not in
application-level Python wrappers in riverbank.

### Proposed additions

**STATE persistence:** pg-tide writes each STATE message to a
`tide.singer_state` table keyed by `(pipeline_name, tap_name)`. On startup,
pg-tide reads the latest STATE and passes it to the tap via `--state` flag (or
a configurable state file path the tap reads at start). This makes every
pg-tide-managed Singer tap automatically incremental across restarts.

```sql
-- Inspect current Singer tap checkpoints.
SELECT * FROM tide.singer_state;
-- pipeline_name, tap_name, state_value (jsonb), written_at

-- Reset a tap to full re-sync.
DELETE FROM tide.singer_state WHERE pipeline_name = 'github-issues';
```

**SCHEMA handling:** pg-tide logs each SCHEMA message to
`tide.singer_schema_log`. A schema change (new properties, changed types) is
detectable by comparing the latest two entries:

```sql
SELECT * FROM tide.singer_schema_log ORDER BY logged_at DESC LIMIT 10;
-- pipeline_name, tap_name, stream_name, schema (jsonb), logged_at
```

When a SCHEMA message introduces a new property that does not exist in the
target inbox table, pg-tide optionally emits a PostgreSQL NOTICE and writes
a `SCHEMA_DRIFT` event to the outbox with the new schema. This lets riverbank's
worker detect schema drift and trigger a connector reconfiguration without
manual inspection. The behaviour is configurable:

```sql
"singer": {
    "on_schema_change": "log"        -- "log" | "emit_event" | "error"
}
```

### riverbank impact

riverbank's Singer connector wrapper no longer needs to implement STATE
management or schema drift detection. `tap-github | pg-tide --target singer
--config tide-singer.json` is fully resumable across restarts without
additional Python code. Schema drift arrives as a typed event on the outbox
and is handled by the riverbank worker's existing event routing.

### Acceptance criteria

- After a restart, a Singer tap managed by pg-tide resumes from the last
  STATE checkpoint; no RECORD messages are re-delivered that were already in
  the inbox before the restart.
- `tide.singer_state` contains one row per `(pipeline_name, tap_name)` with
  the latest STATE JSONB value.
- `DELETE FROM tide.singer_state WHERE pipeline_name = 'x'` causes the next
  tap invocation to emit from the beginning (verified by RECORD count).
- A SCHEMA message adding a new property triggers the configured
  `on_schema_change` behaviour (log / emit event / error).

---

## 6. Inbox rate limiting and backpressure for reverse pipelines

### Background

Reverse pipelines (external source → pg_trickle inbox) can overwhelm a
compilation worker if the source emits faster than riverbank can process.
A Kafka topic replaying a large backlog, or a mass-upload event from a
content management system, can fill the inbox table faster than the downstream
worker drains it. The inbox table grows unboundedly; the riverbank worker
queues runs; latency spikes.

Rate limiting on reverse pipelines is a transport concern. pg-tide controls the
intake rate at the point where external messages enter the inbox. This is the
right control point — not the worker (which would have to read and discard
messages it cannot process) and not Kafka-side (which is outside pg-tide's
control).

### Proposed addition

A `rate_limit` block in `tide.relay_inbox_config`:

```sql
INSERT INTO tide.relay_inbox_config (pipeline_name, enabled, config)
VALUES ('source-ingest', true,
    '{"source_type":    "kafka",
      "kafka_brokers":  "${env:KAFKA_BROKERS}",
      "kafka_topic":    "documents",
      "inbox_table":    "source_inbox",
      "rate_limit": {
          "max_rows_per_second": 50,
          "max_backlog_rows":    5000,
          "on_backlog_exceeded": "pause"
      }
    }'::jsonb);
```

`on_backlog_exceeded` policy:
- `"pause"` — suspend consuming from the external source until inbox backlog
  drains below 80% of `max_backlog_rows` (backpressure)
- `"drop"` — discard incoming messages, incrementing a drop counter metric
  (for non-critical, high-velocity streams)
- `"error"` — close the pipeline and write an error event to the outbox

Backlog depth is measured as the number of unprocessed rows in the inbox
stream table (rows with `processed_at IS NULL`). pg-tide polls this count
at a configurable interval (`backlog_poll_interval_ms`, default 1000).

### riverbank impact

The riverbank worker does not need to implement any throttle-feedback
mechanism. It drains the inbox at its natural pace; pg-tide ensures the
intake rate does not outpace drainage. `riverbank health` surfaces
`pgtide_relay_backlog_depth` from item 4 — the same metric doubles as the
backpressure trigger, so operators see both the operational status and the
cause in a single dashboard.

### Acceptance criteria

- When inbox backlog exceeds `max_backlog_rows`, pg-tide suspends consumption
  from the Kafka topic (or other source); the Kafka consumer group lag
  increases as expected.
- Consumption resumes automatically when backlog drops below 80% of
  `max_backlog_rows` without operator intervention.
- `pgtide_relay_backlog_depth` gauge reflects the actual unprocessed row
  count within `backlog_poll_interval_ms`.
- `on_backlog_exceeded: "drop"` increments a dedicated Prometheus counter
  (`pgtide_relay_dropped_messages_total`) per dropped message.

---

## Summary

| Item | riverbank version affected | Complexity | Priority |
|---|---|---|---|
| Content-based payload routing | v0.4.0 (semantic diff events), v0.7.0 (watchpoints) | Medium | High — agent memory bus and watchpoint alerting depend on it |
| Circuit breakers + retry policies | v0.6.0 | Medium | High — production readiness requires this in the transport layer |
| Dead letter queue | v0.6.0 | Medium | High — complements circuit breakers; prevents silent event loss |
| Per-pipeline Prometheus metrics | v0.6.0 | Low–Medium | High — needed for the relay health Perses dashboard |
| Singer STATE + SCHEMA handling | v0.4.0 | Medium | Medium — required for resumable Singer taps; riverbank can work around it with Python STATE management as interim |
| Inbox rate limiting / backpressure | v0.6.0 | Low–Medium | Medium — protects worker stability under high-volume source events |

Items 1–4 (routing, circuit breakers, DLQ, metrics) form a coherent
production-reliability cluster that should be developed together — they
reference each other (circuit breaker state drives DLQ writes; DLQ depth
appears in metrics; metrics drive the dashboard). The ideal release target
is a single pg-tide version that delivers all four before riverbank v0.6.0
ships.

Items 5–6 (Singer handling and backpressure) are independent and can be
prioritised separately around their respective riverbank version targets.
