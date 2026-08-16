# Error Handling

The pg-tide relay is designed to be resilient in the face of failures. It
retries uncommitted batches and never treats an uncertain checkpoint as
success. The transport guarantee is at-least-once; downstream durable
deduplication is required for an effectively exactly-once outcome.

---

## Error Philosophy

pg_tide's error handling follows a simple principle: **transient errors are
retried, and permanent errors receive an explicit terminal disposition.**
Decode, transform, sink, checkpoint, and DLQ failures retain the source
checkpoint unless the documented DLQ path succeeds atomically.

The relay distinguishes between:

- **Transient errors** — network timeouts, temporary unavailability, connection resets. These will succeed if retried.
- **Permanent errors** — malformed payloads, deserialization failures, invalid configuration. These will never succeed regardless of retries.

---

## Retry Strategy

All transient errors trigger exponential backoff retry with jitter:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| **Initial delay** | 100ms | Start retrying quickly for brief hiccups |
| **Maximum delay** | 30 seconds | Cap the backoff to avoid minute-long waits |
| **Jitter** | ±20% | Prevent thundering herd when multiple relays reconnect simultaneously |
| **Maximum retries** | Unlimited | The relay retries while the source retains the uncommitted batch |
| **Backoff multiplier** | 2× | Each retry doubles the delay (100ms → 200ms → 400ms → ...) |

The backoff sequence looks like: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s, 30s, 30s, 30s...

Jitter randomizes each delay by ±20%, so the actual sequence might be: 85ms, 220ms, 350ms, 900ms, etc. This prevents synchronized retry storms.

---

## Error Categories

### Connection errors (PostgreSQL)

**Symptoms:** Relay logs `"PostgreSQL connection failed, retrying"` or `"postgres error"`

**What happens:**
1. The relay logs a warning with connection details
2. Enters reconnection mode with exponential backoff (100ms → 30s)
3. All pipelines are paused (they can't function without the database)
4. On reconnect, advisory locks are re-acquired
5. Pipeline processing resumes from the last committed offset
6. The source checkpoint remains uncommitted; retry may duplicate a previously
   acknowledged downstream publish

**Common causes:**
- PostgreSQL is restarting or failing over
- Network partition between relay and database
- Connection pool exhaustion
- Authentication failure (password rotation)

**Resolution:** Usually self-healing. The relay reconnects automatically when PostgreSQL is available again. If the issue is persistent (auth failure), fix the credentials and the relay will reconnect on its next attempt.

### Sink errors (delivery failures)

**Symptoms:** Relay logs `"sink publish error"` or `"sink unhealthy"`, Prometheus counter `pg_tide_relay_publish_errors_total` increases.

**What happens:**
1. The source checkpoint remains pending; a prior sink success may be retried
2. The relay retries delivery with exponential backoff until the sink recovers
3. Prometheus metrics track `pg_tide_relay_publish_errors_total{pipeline="..."}`
4. The health endpoint reports unhealthy (`503`) for affected pipelines
5. Once the sink recovers, delivery resumes automatically

**Common causes:**
- Downstream system (Kafka, NATS, webhook endpoint) is temporarily unavailable
- Network issues between relay and sink
- Sink is overloaded and rejecting new messages (backpressure)
- TLS certificate issues

**Resolution:** Usually self-healing. Monitor the error rate and investigate if it persists beyond expected maintenance windows.

### Source errors (reverse mode)

**Symptoms:** Relay logs `"source poll error"` for reverse pipelines.

**What happens:**
1. The relay retries subscription/polling with exponential backoff
2. Once reconnected, consumption resumes from the last acknowledged position
3. No messages are skipped (the source tracks its own offset)

**Common causes:**
- External source (NATS, Kafka, SQS) is temporarily unavailable
- Subscription expired or was revoked
- Consumer group rebalancing (Kafka)

### Payload errors (permanent)

**Symptoms:** Relay logs `"payload decode error"` or `"unsupported outbox payload version"`

**What happens:**
1. The error is logged with full context (outbox name, message ID, raw payload excerpt)
2. The pipeline pauses or routes the complete failed batch to the atomic DLQ
3. The offset advances only after that terminal disposition is durable
4. Prometheus tracks the error count

**Common causes:**
- Application published malformed JSONB that the relay cannot interpret
- Message format version mismatch (relay expects v2, message is v1)
- Corruption (extremely rare)

**Resolution:** Investigate the specific message. Fix the publishing code if it's generating invalid payloads. For format mismatches, upgrade the relay or add backward-compatible handling.

### Configuration errors

**Symptoms:** Relay logs `"config error"` or `"invalid config for pipeline"` at startup or after hot-reload.

**What happens:**
1. If the error is in the TOML file, the relay refuses to start
2. If the error is in a pipeline config (in PostgreSQL), that specific pipeline is skipped
3. Other pipelines continue to operate normally

**Common causes:**
- Missing required config key (e.g., no `brokers` for Kafka sink)
- Invalid value (e.g., non-numeric batch_size)
- Unsupported backend name

**Resolution:** Fix the configuration. For pipeline configs, update the JSONB in the database and the relay will pick up the correction via hot-reload.

---

## Graceful Shutdown

When the relay receives `SIGTERM` or `SIGINT`:

1. **Stop accepting new work** — no new batches are fetched from the outbox
2. **Drain in-flight messages** — wait for currently-delivering batches to complete (up to a drain timeout)
3. **Commit final offsets** — record the last successfully delivered position
4. **Release the worker's ownership session** — only after the worker exits
5. **Close connections** — cleanly disconnect from PostgreSQL and sinks
6. **Exit with code 0** — signal success to the process manager

The drain timeout prevents the relay from hanging indefinitely if a sink is unresponsive during shutdown. Messages that weren't committed will be re-delivered by the next relay instance (and deduplicated by the inbox if applicable).

---

## Dead-Letter Queue (Inbox Side)

For reverse pipelines that write to inboxes, messages that fail processing are managed through the inbox's built-in DLQ mechanism.

### How messages enter the DLQ

1. Your application reads a message from the inbox and attempts to process it
2. Processing fails (external API timeout, validation error, business rule violation)
3. You call `tide.inbox_mark_failed(inbox_name, event_id, error_message)`
4. The message's `retry_count` is incremented and `last_error` is recorded
5. After `max_retries` failures, the message is effectively dead-lettered

### Querying the DLQ

```sql
-- Find all dead-lettered messages in an inbox
SELECT event_id, payload, retry_count, last_error, received_at
FROM tide."my-inbox_inbox"
WHERE processed_at IS NULL
  AND retry_count >= 5  -- assuming max_retries = 5
ORDER BY received_at;
```

### Investigating failures

```sql
-- Group DLQ messages by error pattern
SELECT
  left(last_error, 50) AS error_pattern,
  count(*) AS message_count,
  min(received_at) AS earliest,
  max(received_at) AS latest
FROM tide."my-inbox_inbox"
WHERE processed_at IS NULL AND retry_count >= 5
GROUP BY left(last_error, 50)
ORDER BY message_count DESC;
```

### Replaying messages

After fixing the underlying issue, replay specific messages or all DLQ messages:

```sql
-- Replay specific messages
SELECT tide.replay_inbox_messages('my-inbox',
  ARRAY['evt-001', 'evt-002', 'evt-003']);

-- Replay all DLQ messages for an inbox
SELECT tide.replay_inbox_messages('my-inbox',
  (SELECT array_agg(event_id)
   FROM tide."my-inbox_inbox"
   WHERE processed_at IS NULL AND retry_count >= 5)
);
```

Replaying resets `retry_count` to 0, making messages eligible for normal processing again.

---

## Extension Error Reference

Errors raised by pg_tide SQL functions:

| Error message | Raised by | What it means |
|---------------|-----------|---------------|
| `outbox already exists: {name}` | `outbox_create` | An outbox with this name already exists. Use `p_if_not_exists := true` to suppress. |
| `outbox not found: {name}` | `outbox_publish`, `outbox_drop`, `outbox_status`, `outbox_enable/disable` | No outbox with this name exists. Create it first with `outbox_create`. |
| `inbox already exists: {name}` | `inbox_create` | An inbox with this name already exists. |
| `inbox not found: {name}` | `inbox_drop`, `inbox_mark_processed/failed`, `inbox_status` | No inbox with this name exists. |
| `relay pipeline not found: {name}` | `relay_enable/disable/delete/get_config` | No pipeline with this name in the catalog. |
| `invalid argument: {details}` | Various | A parameter value is invalid (e.g., negative retention_hours). |
| `SPI error: {details}` | Various | Internal database error during SPI execution. |

### Handling extension errors in PL/pgSQL

```sql
DO $$
BEGIN
  PERFORM tide.outbox_publish('maybe-missing', '{}'::jsonb, '{}'::jsonb);
EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE 'Publish failed: %', SQLERRM;
    -- Handle gracefully: log, retry, or use a fallback
END $$;
```

---

## Relay Error Reference

Errors logged by the pg-tide relay binary:

| Error | Category | What it means | Self-healing? |
|-------|----------|---------------|:------------:|
| `postgres error` | Connection | Database communication failure | ✓ (reconnects) |
| `postgres connection failed` | Connection | Cannot reach PostgreSQL | ✓ (retries) |
| `config error` | Configuration | Invalid TOML or missing field | ✗ (fix config) |
| `invalid config for pipeline` | Configuration | Pipeline JSONB validation failure | ✗ (fix SQL config) |
| `pipeline not found` | Configuration | Referenced pipeline doesn't exist | ✗ (create pipeline) |
| `missing required config key` | Configuration | A required backend config key is missing | ✗ (fix SQL config) |
| `unsupported outbox payload version` | Payload | Message format version mismatch | ✗ (upgrade relay or fix publisher) |
| `payload decode error` | Payload | Cannot deserialize message | ✗ (fix publisher) |
| `sink publish error` | Delivery | Sink rejected or timed out | ✓ (retries) |
| `sink unhealthy` | Delivery | Sink not accepting connections | ✓ (retries) |
| `source poll error` | Ingestion | Source read failure | ✓ (retries) |
| `channel closed` | Internal | Internal communication channel dropped | ✓ (relay recovers) |

---

## Monitoring Errors

### Prometheus metrics for error tracking

```promql
# Total delivery errors by pipeline (should be 0 in steady state)
rate(pg_tide_relay_publish_errors_total[5m])

# Unhealthy pipelines (immediate alert)
pg_tide_relay_pipeline_healthy == 0

# Error rate as a percentage of total deliveries
rate(pg_tide_relay_publish_errors_total[5m])
  / rate(pg_tide_relay_messages_published_total[5m])
```

### Alerting rules

```yaml
- alert: PgTideDeliveryErrors
  expr: rate(pg_tide_relay_publish_errors_total[5m]) > 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "Delivery errors on pipeline {{ $labels.pipeline }}"
    description: "The relay is experiencing delivery failures. Check sink availability."

- alert: PgTidePipelineDown
  expr: pg_tide_relay_pipeline_healthy == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} is unhealthy"
    description: "Immediate investigation required. Messages are accumulating."
```

### Log-based monitoring

With structured JSON logging (`--log-format json`), you can filter and alert on error logs:

```json
{"level":"error","pipeline":"orders-to-kafka","error":"sink publish error: BrokerNotAvailable","msg":"delivery failed, will retry","timestamp":"2025-01-15T10:30:00Z"}
```

Key fields to monitor:
- `level=error` — any error-level log indicates a problem
- `pipeline` — identifies which pipeline is affected
- `error` — the specific error message for diagnosis
