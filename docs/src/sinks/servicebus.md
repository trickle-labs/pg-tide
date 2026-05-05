# Azure Service Bus

Azure Service Bus is Microsoft's enterprise message broker, providing reliable message delivery with advanced features like sessions, transactions, dead-lettering, and scheduled delivery. It serves as the backbone for many enterprise integration patterns on Azure. When pg_tide publishes to Service Bus, your outbox messages are delivered to queues or topics where Azure Functions, Logic Apps, or custom applications can process them with enterprise-grade reliability.

## When to Use This Sink

Choose Azure Service Bus when your infrastructure runs on Azure, when you need enterprise messaging features (sessions for ordered processing, transactions, scheduled messages), or when you are integrating with Azure-native services like Azure Functions and Logic Apps. Service Bus is particularly strong for ordered message processing and complex routing scenarios within Azure.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-servicebus',
    'events',
    'servicebus-relay',
    '{
        "sink_type": "servicebus",
        "connection_string": "${env:SERVICEBUS_CONNECTION_STRING}",
        "queue_or_topic": "outbox-events",
        "session_id": "{stream_table}",
        "batch_size": 50
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"servicebus"` |
| `connection_string` | string | — | Service Bus connection string with send permissions |
| `queue_or_topic` | string | — | Target queue or topic name |
| `session_id` | string | `null` | Session ID template for session-enabled entities |
| `batch_size` | int | `50` | Messages per batch send |
| `content_type` | string | `"application/json"` | Message content type |
| `properties` | object | `null` | Custom application properties |

## Delivery Guarantees

Service Bus provides at-least-once delivery with server-side duplicate detection available. When sending to session-enabled queues/topics, messages within the same session are delivered in FIFO order. The relay commits offsets only after Service Bus acknowledges receipt.

## Troubleshooting

- **"Unauthorized access"** — Verify connection string has `Send` permission on the entity
- **"Entity not found"** — Queue/topic does not exist; create it in the Azure portal
- **"Session ID required"** — The target entity requires sessions; set `session_id`

## Further Reading

- [Sources: Service Bus](../sources/servicebus.md) — Receiving from Service Bus
- [Azure Event Hubs](eventhubs.md) — For higher throughput scenarios on Azure
