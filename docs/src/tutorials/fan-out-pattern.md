# Tutorial: Fan-out Pattern

Deliver the same outbox events to multiple downstream systems simultaneously. Each system maintains its own consumer group with independent offset tracking.

---

## The Pattern

```
                    ┌──▶ NATS (real-time notifications)
orders outbox ──▶──┼──▶ Kafka (analytics pipeline)
                    └──▶ Webhook (partner integration)
```

Each destination gets its own relay pipeline and consumer group, progressing independently.

---

## Step 1: Create the Shared Outbox

```sql
SELECT tide.outbox_create('orders', p_retention_hours := 72);
```

## Step 2: Create Consumer Groups

```sql
SELECT tide.create_consumer_group('nats-relay', 'orders');
SELECT tide.create_consumer_group('kafka-relay', 'orders');
SELECT tide.create_consumer_group('webhook-relay', 'orders');
```

## Step 3: Configure Pipelines

```sql
-- To NATS
SELECT tide.relay_set_outbox('orders-nats', 'orders', 'nats',
  '{"url": "nats://nats:4222", "subject": "orders.events"}'::jsonb
);

-- To Kafka
SELECT tide.relay_set_outbox('orders-kafka', 'orders', 'kafka',
  '{"brokers": "kafka:9092", "topic": "orders"}'::jsonb
);

-- To partner webhook
SELECT tide.relay_set_outbox('orders-webhook', 'orders', 'webhook',
  '{"url": "https://partner.example.com/hooks/orders"}'::jsonb
);
```

## Step 4: Monitor Independent Progress

```sql
SELECT * FROM tide.consumer_lag;
```

```
 group_name    | outbox_name | consumer_id | committed_offset | lag
---------------+-------------+-------------+------------------+-----
 nats-relay    | orders      | relay-0     |             1000 |   0
 kafka-relay   | orders      | relay-0     |              950 |  50
 webhook-relay | orders      | relay-0     |              800 | 200
```

The webhook relay is behind (maybe the partner endpoint is slow) — but it doesn't affect NATS or Kafka delivery.

---

## Benefits

- **Independent progress** — slow consumers don't block fast ones
- **Independent retry** — if one sink fails, others continue
- **Single source of truth** — all events come from the same outbox
- **No message duplication** at the source — publish once, deliver many
