# NATS JetStream

The NATS sink publishes native pg_tide JSON or CloudEvents from the PostgreSQL
outbox to a JetStream subject. It is outbound only.

```json
{
  "source_type": "outbox",
  "source": {"outbox": "orders"},
  "sink_type": "nats",
  "sink": {
    "url": "nats://nats:4222",
    "subject": "orders.events"
  },
  "wire_format": "native"
}
```

Use `subject_template` when the subject should include outbox metadata.
JetStream acknowledgments and the stable outbox identity provide at-least-once
delivery. `Nats-Msg-Id` suppresses a retry only while the stream's configured
duplicate window is active; a retry after that window may appear twice.
