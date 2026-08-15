# Azure Event Hubs

Azure Event Hubs is a high-throughput event ingestion service on Azure, capable of receiving and processing millions of events per second. It is architecturally similar to Apache Kafka — events flow into partitions, consumer groups track progress independently, and data is retained for a configurable period. In fact, Event Hubs exposes a Kafka-compatible endpoint, making it a managed alternative to self-hosted Kafka on Azure. When pg_tide publishes to Event Hubs, your outbox messages become available to Azure Stream Analytics, Azure Functions, and any Kafka-compatible consumer.

## When to Use This Sink

Choose Event Hubs when you need high-throughput event ingestion on Azure, when you want a managed Kafka-compatible service without cluster operations, or when you are building real-time analytics pipelines with Azure Stream Analytics. Event Hubs is designed for millions of events per second and integrates natively with the Azure data ecosystem.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-eventhubs',
    'outbox', 'events',
    'sink_type', 'eventhubs',
    'config', '{
        "connection_string": "${env:EVENTHUBS_CONNECTION_STRING}",
        "event_hub_name": "outbox-events",
        "partition_key": "{stream_table}",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"eventhubs"` |
| `connection_string` | string | — | Event Hubs connection string |
| `event_hub_name` | string | — | Target Event Hub name |
| `partition_key` | string | `null` | Partition key template for ordering |
| `batch_size` | int | `100` | Events per batch send |
| `properties` | object | `null` | Custom event properties |

## Delivery Guarantees

Event Hubs provides at-least-once delivery. Events are durably stored across multiple replicas before the send is acknowledged. Ordering is guaranteed within a partition (determined by partition key).

## Kafka Compatibility

Event Hubs exposes a Kafka-compatible endpoint. If you prefer, you can use the [Kafka sink](kafka.md) with Event Hubs' Kafka endpoint instead of the native Event Hubs protocol. The native sink is slightly more efficient as it uses the AMQP protocol directly.

## Troubleshooting

- **"Unauthorized"** — Connection string needs `Send` claim on the Event Hub
- **"Event Hub not found"** — Verify the event hub name matches the entity in your namespace
- **"Quota exceeded"** — Throughput units exhausted; scale up the Event Hubs namespace

## Further Reading

- [Sources: Event Hubs](../sources/eventhubs.md) — Consuming from Event Hubs
- [Azure Service Bus](servicebus.md) — For enterprise queuing patterns
