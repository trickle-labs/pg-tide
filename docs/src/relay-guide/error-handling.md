# Error Handling

The pg-tide relay is designed to be resilient in the face of failures. This page describes how errors are handled at each stage of the pipeline.

---

## Retry Strategy

All transient errors trigger exponential backoff retry:

- **Initial delay:** 100ms
- **Max delay:** 30 seconds
- **Jitter:** ±20% to prevent thundering herd
- **Max retries:** Unlimited (the relay retries forever for transient errors)

---

## Error Categories

### Connection Errors (PostgreSQL)

If the database connection drops:

1. The relay logs a warning and enters reconnection mode
2. Exponential backoff with jitter (100ms → 30s)
3. On reconnect, advisory locks are re-acquired
4. Pipeline processing resumes from last committed offset

### Sink Errors (Delivery Failures)

If the downstream sink is unavailable:

1. Messages remain pending in the outbox (not lost)
2. The relay retries with backoff until the sink recovers
3. Metrics track `pg_tide_relay_publish_errors_total`
4. Health endpoint reports unhealthy

### Source Errors (Reverse Mode)

If an external source is unavailable:

1. The relay retries subscription/polling with backoff
2. Once reconnected, consumption resumes from last acknowledged position

### Payload Errors

If a message cannot be deserialized or transformed:

1. The error is logged with full context (outbox name, message ID)
2. The message is skipped (not retried — it won't succeed)
3. Metrics track the error

---

## Graceful Shutdown

On `SIGTERM` or `SIGINT`:

1. Stop accepting new work
2. Wait for in-flight messages to complete delivery (drain timeout)
3. Commit final offsets
4. Release all advisory locks
5. Exit cleanly

---

## Dead Letter Queue (Inbox Side)

For reverse pipelines with inbox delivery, messages that fail processing more than `max_retries` times are held in the inbox table as DLQ entries. Use `tide.replay_inbox_messages()` to re-queue them after fixing the issue.

---

## Monitoring Errors

```promql
# Total publish errors by pipeline
rate(pg_tide_relay_publish_errors_total[5m])

# Unhealthy pipelines
pg_tide_relay_pipeline_healthy == 0
```

See [Monitoring](monitoring.md) for the full metrics reference.
