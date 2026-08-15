# Feature: Transforms

Transforms let you filter and reshape messages in-flight — between polling from the source and publishing to the sink. Using JMESPath expressions, you can drop messages that don't match a condition, extract specific fields from a payload, reshape the data structure, or compute derived values. All without touching your application code or database schema.

## Two Operations

Transforms provide two complementary operations:

**Filter** — A JMESPath expression evaluated as a predicate. If the result is "truthy" (not null, not false, not empty), the message passes through. If "falsy", the message is silently dropped and acknowledged.

**Payload projection** — A JMESPath expression that replaces the entire message payload with its result. The original payload goes in; the expression's output comes out.

You can use filter alone, projection alone, or both together (filter is applied first).

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'high-value-orders',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "high-value-orders",
        "transform": {
            "filter": "payload.total > `1000`",
            "payload": "{ order_id: payload.id, amount: payload.total, customer: payload.customer_email }"
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `transform.filter` | string | `null` | JMESPath filter expression (truthy = keep) |
| `transform.payload` | string | `null` | JMESPath projection expression (replaces payload) |

## JMESPath Quick Reference

JMESPath is a query language for JSON. Here are the patterns most useful for transforms:

### Field Access
```
payload.order_id          → "ORD-001"
payload.customer.email    → "alice@example.com"
```

### Comparisons (for filters)
```
payload.status == 'confirmed'     → true/false
payload.amount > `100`            → true/false (backtick for literals)
payload.priority != 'low'         → true/false
```

### Boolean Logic (for filters)
```
payload.status == 'confirmed' && payload.amount > `100`
payload.region == 'us' || payload.region == 'eu'
```

### Object Projection (for payloads)
```
{
  id: payload.order_id,
  total: payload.amount,
  email: payload.customer.email
}
```

### Field Existence (for filters)
```
payload.premium_tier        → truthy if field exists and is not null
```

## Examples

### Filter: Only forward error events
```json
{
  "transform": {
    "filter": "payload.level == 'error'"
  }
}
```

### Filter: Drop internal events
```json
{
  "transform": {
    "filter": "payload.source != 'internal'"
  }
}
```

### Projection: Slim down payload
```json
{
  "transform": {
    "payload": "{ id: payload.id, type: payload.event_type, data: payload.data }"
  }
}
```

### Combined: Filter and reshape
```json
{
  "transform": {
    "filter": "payload.country == 'US' && payload.amount > `50`",
    "payload": "{ order: payload.id, amount: payload.amount, state: payload.shipping.state }"
  }
}
```

## Truthiness Rules

JMESPath truthiness determines whether a filter passes:

| Value | Truthy? | Example |
|-------|---------|---------|
| `null` | No | Missing field |
| `false` | No | Failed comparison |
| `""` (empty string) | No | Empty text field |
| `[]` (empty array) | No | No items |
| `{}` (empty object) | No | No fields |
| Everything else | Yes | Numbers, non-empty strings, arrays with items |

## Performance

Transforms are applied in-memory before publishing. JMESPath expressions are compiled once at pipeline startup and evaluated per-message. The overhead is negligible for typical expressions — a few microseconds per message.

For filters that drop many messages, transforms reduce load on the sink (fewer messages to publish) while the source continues polling at full speed.

## Further Reading

- [Routing](routing.md) — Content-based topic routing (complementary feature)
- [JMESPath specification](https://jmespath.org/) — Full language reference
