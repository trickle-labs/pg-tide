# Tutorial: Getting Started with pg_tide

This tutorial takes you from zero to a working pg_tide pipeline in 10 minutes. You'll create an outbox, publish events, configure a relay pipeline, and see messages delivered to a sink.

## Prerequisites

- PostgreSQL 18
- The pg_tide extension installed (`CREATE EXTENSION pg_tide`)
- The `pg-tide` relay binary (see [Installation](../getting-started/installation.md))

## Step 1: Install the Extension

```sql
CREATE EXTENSION pg_tide;
```

This creates the `tide` schema with all catalog tables and SQL functions.

## Step 2: Create an Outbox

```sql
SELECT tide.outbox_create('my_events');
```

This creates an outbox table that will store your events until they're relayed.

## Step 3: Publish an Event

```sql
SELECT tide.outbox_publish('my_events', 'user-signups', '{
    "user_id": "USR-001",
    "email": "alice@example.com",
    "plan": "pro"
}'::jsonb);
```

The event is now stored in the outbox. It's part of your current transaction — if you ROLLBACK, the event disappears too. That's the transactional outbox guarantee.

## Step 4: Check the Outbox

```sql
SELECT * FROM tide.outbox_status('my_events');
```

You'll see one pending event waiting to be relayed.

## Step 5: Configure a Relay Pipeline

For this tutorial, we'll use the `stdout` sink (prints messages to the relay's terminal):

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'my-first-pipeline',
    'outbox', 'my_events',
    'sink_type', 'stdout',
    'config', '{
        "format": "json_pretty"
    }'::jsonb
  )
);
```

## Step 6: Start the Relay

In a terminal:

```bash
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

You should see your event printed to the terminal:

```json
{
  "outbox_id": 1,
  "op": "insert",
  "stream_table": "user-signups",
  "payload": {
    "user_id": "USR-001",
    "email": "alice@example.com",
    "plan": "pro"
  }
}
```

## Step 7: Publish More Events

With the relay running, publish additional events and watch them appear in real-time:

```sql
SELECT tide.outbox_publish('my_events', 'user-signups', '{
    "user_id": "USR-002",
    "email": "bob@example.com",
    "plan": "free"
}'::jsonb);
```

## Next Steps

Now that you have a working pipeline, try:

- **Switch to a real sink:** Replace `stdout` with [Kafka](../sinks/kafka.md), [NATS](../sinks/nats.md), or any other [supported sink](../sinks/overview.md)
- **Add transforms:** [Filter and reshape](../features/transforms.md) messages before delivery
- **Create an inbox:** [Receive events](../sql-reference/inbox-api.md) from external systems
- **Set up monitoring:** [Prometheus metrics](../features/metrics.md) for production visibility

## Further Reading

- [First Pipeline (detailed)](../getting-started/first-pipeline.md) — Extended getting-started guide
- [Concepts: Transactional Outbox](../concepts/transactional-outbox.md) — Why this pattern works
- [SQL Reference: Outbox API](../sql-reference/outbox-api.md) — Complete function reference
