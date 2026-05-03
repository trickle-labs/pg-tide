# Quickstart

Get pg_tide running in 5 minutes. By the end of this page you'll have an outbox publishing messages and a relay delivering them to stdout.

---

## 1. Install the Extension

```sql
CREATE EXTENSION pg_tide;
```

## 2. Create an Outbox

```sql
SELECT tide.outbox_create('events',
  p_retention_hours := 24,
  p_inline_threshold := 10000
);
```

This registers a logical outbox named `events`. Messages published here are stored in `tide.tide_outbox_messages` and retained for 24 hours.

## 3. Publish Some Messages

```sql
-- Publish within a transaction alongside your business logic.
BEGIN;
  INSERT INTO my_table (id, data) VALUES (1, 'hello');

  SELECT tide.outbox_publish('events',
    '{"id": 1, "action": "created", "data": "hello"}'::jsonb,
    '{"event_type": "my_table.created"}'::jsonb
  );
COMMIT;
```

The message is now atomically committed with your business data. No dual-write risk.

## 4. Check the Outbox

```sql
SELECT * FROM tide.outbox_pending;
```

```
 outbox_name | pending_count |       oldest_at        | max_id
-------------+---------------+------------------------+--------
 events      |             1 | 2025-01-15 10:30:00+00 |      1
```

## 5. Start the Relay

The relay binary reads pipeline config from the database and delivers messages to configured sinks. For this quickstart, we'll use stdout:

```bash
# Configure a forward pipeline (outbox → stdout)
psql -c "SELECT tide.relay_set_outbox('events-stdout', 'events', 'stdout');"

# Start the relay
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

The relay will start polling the `events` outbox and printing messages to stdout. In production, you'd configure a real sink (NATS, Kafka, webhooks, etc.).

## 6. Create a Consumer Group

Consumer groups let you track which messages have been consumed:

```sql
SELECT tide.create_consumer_group('my-service', 'events');
```

Check consumer lag:

```sql
SELECT * FROM tide.consumer_lag;
```

---

## What Just Happened?

1. The extension created catalog tables in the `tide` schema
2. You published a message atomically within a business transaction
3. The relay picked up the message and delivered it downstream
4. Consumer offsets track progress across restarts

---

## Next Steps

- [Full Tutorial →](tutorial.md) — build a complete pipeline with NATS
- [SQL Reference →](../sql-reference/outbox-api.md) — all outbox functions
- [Relay Configuration →](../relay-guide/configuration.md) — TOML config and CLI flags
