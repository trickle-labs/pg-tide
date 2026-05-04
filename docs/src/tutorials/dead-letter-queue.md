# Dead-Letter Queue

Even the most reliable systems occasionally encounter messages they cannot
process: a malformed payload, a downstream service that is permanently down, or
a bug in the consumer. pg_tide's idempotent inbox gives you a first-class
dead-letter queue (DLQ) built directly into PostgreSQL — no extra broker
configuration required.

## How the DLQ works

When a message fails more than `max_retries` times, `tide.inbox_mark_failed()`
moves it to a separate retention bucket tracked by the `dlq_retention_hours`
column. The message stays visible in the inbox table with `processed_at = NULL`
and a non-empty `last_error`, making it trivial to query, investigate, and
replay.

```
Normal flow:
  received → pending → mark_processed ──► deleted after processed_retention_hours

Failure flow:
  received → pending → mark_failed (retry_count++)
                            │
                    retry_count > max_retries?
                            │
                           YES
                            ▼
                    last_error set, stays in table
                    until dlq_retention_hours expires
```

## Setting up an inbox with a DLQ

```sql
SELECT tide.inbox_create(
    'payments',
    'tide',                -- schema
    3,                     -- max_retries before DLQ
    72,                    -- processed_retention_hours (3 days)
    168                    -- dlq_retention_hours (7 days)
);
```

With `max_retries = 3`, the relay will attempt to process each message up to
three times. After the third failure, `last_error` is populated and the message
is left in the DLQ section of the inbox.

## Viewing DLQ messages

```sql
SELECT
    event_id,
    source,
    payload,
    retry_count,
    last_error,
    received_at
FROM tide.payments_inbox
WHERE processed_at IS NULL
  AND retry_count >= 3
ORDER BY received_at;
```

## Replaying failed messages

Once you have fixed the root cause, replay individual events or entire batches:

```sql
-- Replay a single event:
SELECT tide.replay_inbox_messages('payments', ARRAY['evt-abc-123']);

-- Replay all DLQ messages at once:
SELECT tide.replay_inbox_messages(
    'payments',
    ARRAY(
        SELECT event_id
        FROM tide.payments_inbox
        WHERE processed_at IS NULL AND retry_count >= 3
    )
);
```

`replay_inbox_messages` resets `retry_count` to 0 and clears `last_error`,
making the events eligible for re-processing on the next relay poll.

## Alerting on DLQ depth

Wire the DLQ depth to your alerting stack using a simple SQL query:

```sql
-- Prometheus-style metric via pg_stat_statements or a custom exporter:
SELECT
    inbox_name,
    COUNT(*) AS dlq_depth
FROM tide.tide_inbox_config cfg
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS cnt
    FROM tide.tide_inbox_messages msg
    WHERE msg.processed_at IS NULL
      AND msg.retry_count >= cfg.max_retries
) sub
WHERE cnt > 0;
```

Or, if you use Alertmanager with a Postgres exporter, add a rule:

```yaml
# prometheus-rules.yaml
groups:
  - name: pg_tide
    rules:
      - alert: InboxDLQNotEmpty
        expr: pg_tide_dlq_depth > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "pg_tide inbox {{ $labels.inbox }} has {{ $value }} dead-lettered messages"
```

## Forwarding DLQ messages to an external queue

For long-term storage or cross-team visibility, you can forward DLQ messages to
an external queue (e.g. an S3 bucket or a dedicated Slack alert):

```sql
-- Notify via LISTEN/NOTIFY when a message exceeds max_retries:
CREATE OR REPLACE FUNCTION tide.notify_dlq() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.retry_count >= (
        SELECT max_retries FROM tide.tide_inbox_config
        WHERE inbox_name = TG_TABLE_NAME::text
    ) THEN
        PERFORM pg_notify(
            'tide_dlq',
            json_build_object(
                'inbox',    TG_TABLE_NAME,
                'event_id', NEW.event_id,
                'error',    NEW.last_error
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$;
```

The relay's reverse pipeline can listen on `tide_dlq` and route messages to
a dedicated dead-letter topic in NATS or Kafka for out-of-band handling.

## Automatic DLQ cleanup

DLQ rows are removed automatically when `dlq_retention_hours` expires via
`tide.inbox_truncate_processed()`. You can also clean them manually:

```sql
-- Remove DLQ rows older than 7 days from the payments inbox:
SELECT tide.inbox_truncate_processed('payments');
```
