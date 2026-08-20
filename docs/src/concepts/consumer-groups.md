# Concept: Consumer Groups

Consumer groups allow multiple independent consumers to process messages from the same outbox, each maintaining its own position. This enables fan-out patterns where a single stream of events is consumed by different services at different speeds, without them interfering with each other.

## The Problem

Without consumer groups, a single outbox has a single "cursor" — one position tracking which messages have been delivered. If you want two services to receive the same events (say, an analytics service and a notification service), you'd need to create two separate outboxes and publish events to both.

## The Solution

Consumer groups give each consumer its own independent position within the same outbox:

```
Outbox: order_events
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │10 │
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
                    ↑               ↑
          Analytics group      Notifications group
          (position: 4)        (position: 8)
```

The analytics service processes messages slowly (heavy aggregation), while the notification service processes them quickly. Each advances independently.

## Creating Consumer Groups

```sql
-- Create an outbox
SELECT tide.outbox_create('order_events');

-- Create consumer groups for different services
SELECT tide.consumer_group_create('order_events', 'analytics');
SELECT tide.consumer_group_create('order_events', 'notifications');
SELECT tide.consumer_group_create('order_events', 'search-indexer');
```

## Configuring Pipelines per Group

Each consumer group gets its own relay pipeline:

```sql
-- Analytics: sends to data warehouse (slow, large batches)
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-analytics',
    'outbox', 'order_events',
    'sink_type', 'bigquery',
    'config', '{
        "consumer_group": "analytics",
        "batch_size": 1000,
        "dataset": "raw_events",
        "table": "orders"
    }'::jsonb
  )
);

-- Notifications: sends to Slack (fast, small batches)
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-notifications',
    'outbox', 'order_events',
    'sink_type', 'slack',
    'config', '{
        "consumer_group": "notifications",
        "batch_size": 1,
        "webhook_url": "${env:SLACK_WEBHOOK}"
    }'::jsonb
  )
);

-- Search: sends to Elasticsearch (medium speed)
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-search',
    'outbox', 'order_events',
    'sink_type', 'elasticsearch',
    'config', '{
        "consumer_group": "search-indexer",
        "batch_size": 100,
        "url": "http://elasticsearch:9200",
        "index": "orders"
    }'::jsonb
  )
);
```

## Independent Processing

Each consumer group:
- Tracks its own position (last delivered outbox ID)
- Advances at its own pace
- Has its own circuit breaker state
- Can have different rate limits and delivery settings
- Can target different sinks

If one pipeline falls behind, independent consumer groups continue unaffected.

## Checking Group Status

```sql
-- See position and lag for each consumer group
SELECT * FROM tide.consumer_group_status('order_events');
```

Returns:
| group_name | last_delivered_id | pending_count |
|-----------|------------------:|---------------:|
| analytics | 4,231 | 1,769 |
| notifications | 5,998 | 2 |
| search-indexer | 5,500 | 500 |

## Adding a New Consumer Group

When you add a new consumer group, you choose where it starts:

```sql
-- Start from the beginning (process all historical events)
SELECT tide.consumer_group_create('order_events', 'new-service', 0);

-- Start from the current position (only future events)
SELECT tide.consumer_group_create('order_events', 'new-service');
```

## Use Cases

- **Fan-out:** Same events go to Kafka, Elasticsearch, and a data lake
- **Selective processing:** Each group applies different filters/transforms
- **Speed isolation:** Fast consumers aren't blocked by slow ones
- **A/B testing:** Two groups process the same events with different logic
- **Migration:** New consumer group processes alongside old one during transition

## Further Reading

- [SQL Reference: Consumer Groups API](../sql-reference/consumer-groups-api.md) — Complete function reference
- [Fan-Out Pattern](../tutorials/fan-out-pattern.md) — Tutorial with multiple consumers
- [Notification Fan-Out](../tutorials/notification-fanout.md) — Multi-channel notification example
