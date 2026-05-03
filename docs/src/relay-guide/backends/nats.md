# NATS Backend

[NATS](https://nats.io) is the default and recommended backend for pg_tide. It provides low-latency publish/subscribe with optional JetStream durability.

---

## Forward (Outbox → NATS)

```sql
SELECT tide.relay_set_outbox('orders-nats', 'orders', 'nats',
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'orders.events'
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | NATS server URL |
| `subject` | Yes | — | Subject to publish to (supports templates) |
| `credentials` | No | — | Path to NATS credentials file |

### Subject Templates

The subject supports variable substitution:

- `{outbox_name}` — source outbox name
- `{event_type}` — from headers `event_type` field
- `{outbox_id}` — message ID

Example: `"orders.{event_type}"` → `"orders.order.created"`

---

## Reverse (NATS → Inbox)

```sql
SELECT tide.relay_set_inbox('nats-to-inbox', 'incoming-events',
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'external.events.>'
  ),
  p_source := 'nats'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | NATS server URL |
| `subject` | Yes | — | Subject to subscribe to (wildcards supported) |
| `queue_group` | No | — | Queue group for load balancing |
| `credentials` | No | — | Path to NATS credentials file |

---

## Cargo Feature

Enabled by default. To build without NATS:

```bash
cargo build --package pg-tide-relay --no-default-features --features "stdout"
```
