# Source: NATS JetStream

The NATS source subscribes to NATS JetStream subjects and delivers messages into a pg_tide inbox. This enables reverse pipelines where events from NATS-connected services flow reliably into your PostgreSQL database.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'events-from-nats',
    'inbox', 'incoming_events',
    'source', 'nats',
    'config', '{
        "url": "tls://nats.example:4222",
        "subject": "events.>",
        "durable_name": "pg-tide-consumer",
        "stream": "EVENTS",
        "ack_wait_secs": 30
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"nats"` |
| `url` | string | — | NATS server URL |
| `subject` | string | — | Subject filter (supports wildcards: `*`, `>`) |
| `durable_name` | string | — | Durable consumer name for offset tracking |
| `stream` | string | `null` | JetStream stream name |
| `ack_wait_secs` | int | `30` | Seconds before unacknowledged messages are redelivered |
| `credentials_file` | string | `null` | NATS credentials file |
| `batch_size` | int | `100` | Messages per inbox insert batch |

## Delivery Guarantees

The NATS source acknowledges messages only after they are written to the
inbox. JetStream's durable consumer retains unacknowledged messages subject to
stream retention, and the inbox's deduplication mechanism (using the stable
message identity) makes redelivery safe at the database boundary.

## Troubleshooting

- **"Consumer not found"** — Create the consumer or ensure the stream and subject match
- **"No messages"** — Verify the subject filter matches what publishers are sending to
- **High redelivery count** — The relay may be slow to acknowledge; increase `ack_wait_secs`

## Further Reading

- [Sinks: NATS](../sinks/nats.md) — Publishing to NATS (forward direction)
- [Bidirectional Sync](../tutorials/bidirectional-sync.md) — Two-way NATS communication
