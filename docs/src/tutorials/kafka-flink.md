# Tutorial: Kafka + Flink Stream Processing

This tutorial shows how to build a real-time analytics pipeline using pg_tide and Apache Flink. You'll publish order events from PostgreSQL to Kafka via pg_tide, then process them with Flink SQL to compute running totals and write results back.

## What You'll Build

```
PostgreSQL (orders)  →  pg_tide  →  Kafka  →  Flink SQL  →  Kafka (results)  →  pg_tide  →  PostgreSQL (analytics)
```

## Prerequisites

- PostgreSQL 18 with pg_tide extension installed
- pg-tide relay binary
- Apache Kafka (or Redpanda)
- Apache Flink 1.17+ with SQL Gateway

## Step 1: Create the Outbox

```sql
-- Create an outbox for order events
SELECT tide.outbox_create('order_events');

-- Insert some test events
SELECT tide.outbox_publish('order_events', 'orders', jsonb_build_object(
    'order_id', 'ORD-001',
    'customer_id', 'CUST-42',
    'total', 149.99,
    'region', 'us-east',
    'created_at', now()
));
```

## Step 2: Configure the Pipeline

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "order-events",
        "wire_format": "debezium",
        "wire_config": {
            "server_name": "production"
        }
    }'::jsonb
  )
);
```

## Step 3: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

## Step 4: Create Flink SQL Job

Connect to Flink SQL Gateway and define a source table reading from Kafka:

```sql
CREATE TABLE order_events (
    order_id STRING,
    customer_id STRING,
    total DECIMAL(10, 2),
    region STRING,
    created_at TIMESTAMP(3),
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'order-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-analytics',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset'
);
```

Define the output table:

```sql
CREATE TABLE order_analytics (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    region STRING,
    order_count BIGINT,
    total_revenue DECIMAL(12, 2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'order-analytics',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);
```

Run the aggregation:

```sql
INSERT INTO order_analytics
SELECT 
    window_start,
    window_end,
    region,
    COUNT(*) as order_count,
    SUM(total) as total_revenue
FROM TABLE(
    TUMBLE(TABLE order_events, DESCRIPTOR(created_at), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, region;
```

## Step 5: Ingest Results Back to PostgreSQL

Create an inbox and configure a reverse pipeline to consume Flink's output:

```sql
-- Create inbox for analytics results
SELECT tide.inbox_create('analytics_results');

-- Configure pipeline to consume from the results topic
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'analytics-from-flink',
    'inbox', 'analytics_results',
    'source', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "order-analytics",
        "consumer_group": "pg-tide-analytics",
        "auto_offset_reset": "earliest"
    }'::jsonb
  )
);
```

## Step 6: Query Results

```sql
SELECT payload->>'region' as region,
       (payload->>'total_revenue')::decimal as revenue,
       (payload->>'order_count')::int as orders,
       payload->>'window_end' as period
FROM tide.inbox_pending('analytics_results')
ORDER BY period DESC;
```

## Key Takeaways

- pg_tide's Debezium wire format integrates seamlessly with Flink's `debezium-json` format
- The bidirectional flow (outbox → Kafka → Flink → Kafka → inbox) keeps PostgreSQL as the source of truth
- Flink provides windowed aggregations that would be expensive to compute in PostgreSQL

## Further Reading

- [Sinks: Kafka](../sinks/kafka.md) — Kafka sink configuration
- [Sources: Kafka](../sources/kafka.md) — Kafka source configuration
- [Wire Format: Debezium](../wire-formats/debezium.md) — Debezium compatibility
