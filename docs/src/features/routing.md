# Feature: Content-Based Routing

Content-based routing dynamically determines the destination subject (topic, queue, channel) for each message based on its payload content. Instead of sending all messages from an outbox to a single topic, you can route them to different destinations based on event type, priority, region, or any other field in the message.

## How It Works

Routing rules are evaluated in order. Each rule matches a field in the message payload against an expected value. The first rule that matches determines the output subject. If no rule matches, the default template is used.

```
Message payload:                   Routing rules:
{                                  1. event_type == "order.created" → orders.created
  "event_type": "order.shipped",   2. event_type == "order.shipped" → orders.shipped
  "region": "eu",                  3. region == "eu" → eu.events
  "priority": "high"               default → tide.{stream_table}
}
                                   Result: "orders.shipped" (rule 2 matches first)
```

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'multi-topic-orders',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "routing": {
            "default_template": "orders.general",
            "rules": [
                {
                    "match_field": "event_type",
                    "match_value": "order.created",
                    "subject": "orders.created"
                },
                {
                    "match_field": "event_type",
                    "match_value": "order.shipped",
                    "subject": "orders.shipped"
                },
                {
                    "match_field": "priority",
                    "match_value": "high",
                    "subject": "high-priority.orders"
                }
            ]
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `routing.default_template` | string | `"{stream_table}"` | Fallback subject when no rule matches |
| `routing.rules` | array | `[]` | Ordered list of routing rules |
| `routing.rules[].match_field` | string | — | Dot-separated path into payload |
| `routing.rules[].match_value` | string | — | Expected value (string equality) |
| `routing.rules[].subject` | string | — | Subject template when rule matches |

## Subject Templates

Subject strings support template variables that are expanded at runtime:

| Variable | Expansion |
|----------|-----------|
| `{stream_table}` | The outbox's stream table name |
| `{op}` | Operation type: `insert`, `update`, `delete` |
| `{outbox_id}` | Numeric outbox row ID |

Examples:
- `"orders.{op}"` → `"orders.insert"`, `"orders.update"`, `"orders.delete"`
- `"{stream_table}.{op}"` → `"order_events.insert"`
- `"priority.{stream_table}"` → `"priority.order_events"`

## Nested Field Access

The `match_field` parameter supports dot-separated paths for nested objects:

```json
{
  "routing": {
    "rules": [
      {
        "match_field": "customer.tier",
        "match_value": "enterprise",
        "subject": "enterprise-events"
      },
      {
        "match_field": "shipping.country",
        "match_value": "US",
        "subject": "domestic-shipping"
      }
    ]
  }
}
```

## Rule Evaluation

- Rules are evaluated **in order** — first match wins
- If no rule matches, the `default_template` is used
- Field matching is **string equality** (case-sensitive)
- Missing fields never match (treated as null, which ≠ any string)

## Use Cases

### Fan-out by event type
Route different event types to dedicated topics for independent consumers:
```json
{
  "rules": [
    { "match_field": "type", "match_value": "user.signup", "subject": "user-signups" },
    { "match_field": "type", "match_value": "user.churn", "subject": "user-churn" },
    { "match_field": "type", "match_value": "order.placed", "subject": "new-orders" }
  ]
}
```

### Priority routing
Send high-priority messages to a fast-lane topic with dedicated consumers:
```json
{
  "rules": [
    { "match_field": "priority", "match_value": "critical", "subject": "alerts.critical" },
    { "match_field": "priority", "match_value": "high", "subject": "alerts.high" }
  ],
  "default_template": "alerts.normal"
}
```

### Geographic routing
Route events to region-specific topics:
```json
{
  "rules": [
    { "match_field": "region", "match_value": "eu", "subject": "events.eu" },
    { "match_field": "region", "match_value": "us", "subject": "events.us" },
    { "match_field": "region", "match_value": "apac", "subject": "events.apac" }
  ]
}
```

## Further Reading

- [Transforms](transforms.md) — Filter and reshape messages (applied before routing)
- [Fan-Out Pattern](../tutorials/fan-out-pattern.md) — Tutorial combining routing with multiple consumers
