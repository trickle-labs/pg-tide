# Migrating from Kafka Connect to pg-tide + DuckLake

This tutorial shows how to migrate a Kafka Connect → S3 pipeline to pg-tide +
DuckLake, simplifying your stack while gaining exactly-once delivery, richer
SQL querying, and lower operational overhead.

## Why Migrate?

| | Kafka Connect | pg-tide + DuckLake |
|---|---|---|
| **Events source** | Kafka topic (separate write + read) | PostgreSQL outbox (atomic with business logic) |
| **Exactly-once** | Complex, requires Kafka transactions | Built-in via `atomic_lake_writes = true` |
| **Query engine** | Separate (Spark, Trino, Athena) | DuckDB directly (no extra service) |
| **Delivery guarantee** | At-least-once by default | Exactly-once with pg-tide |
| **Schema evolution** | Confluent Schema Registry required | Built-in via DuckLake `ducklake_column` |
| **Operations** | Kafka cluster + Connect workers + Schema Registry | Existing PostgreSQL |

## Migration Checklist

- [ ] Identify Kafka Connect source connectors (JDBC source, Debezium, etc.)
- [ ] Map Kafka topics to pg-tide outboxes
- [ ] Configure DuckLake S3 path to match existing Parquet layout (or choose new)
- [ ] Run both pipelines in parallel to validate event parity
- [ ] Cut over Kafka consumers to DuckDB queries
- [ ] Decommission Kafka Connect workers

## Step 1: Create Equivalent Outboxes

For each Kafka topic `orders.events`, create a pg-tide outbox:

```sql
SELECT tide.outbox_create('orders_events', 'payload JSONB NOT NULL');
```

## Step 2: Update Application Code

Replace Kafka producer calls with `tide.outbox_publish()`:

```sql
-- Before (application publishes to Kafka via client library):
-- kafkaProducer.send("orders.events", orderId, orderJson);

-- After (publish inside the same database transaction):
PERFORM tide.outbox_publish('orders_events', order_json::jsonb);
```

## Step 3: Configure DuckLake Sink

```sql
SELECT tide.relay_set_outbox_v2(jsonb_build_object(
    'name',       'orders-events-lake',
    'outbox',     'orders_events',
    'sink_type',  'ducklake',
    'batch_size', 100,
    'enabled',    true,
    'config', jsonb_build_object(
        'data_path',          's3://my-data-lake/orders/events/',
        'namespace',          'pgtide',
        'table_template',     'orders_events',
        'atomic_lake_writes', true,
        'partition',          'daily'
    )
));
```

## Step 4: Validate Parity

During the migration window, run both pipelines. Use DuckDB to compare:

```sql
-- Count events from Kafka Connect output.
SELECT COUNT(*) FROM iceberg.my_catalog.orders_events;

-- Count events from pg-tide DuckLake.
SELECT COUNT(*) FROM lake.pgtide.orders_events;
```

They should converge within seconds (Kafka Connect has higher latency).

## Step 5: Migrate Consumers

Replace Spark/Trino/Athena queries with DuckDB:

```sql
-- Before (Trino query against Hive Metastore):
-- SELECT * FROM hive.orders.events WHERE dt = '2026-05-19' LIMIT 100;

-- After (DuckDB against DuckLake):
SELECT * FROM lake.pgtide.orders_events
WHERE created_at::date = '2026-05-19'
LIMIT 100;
```

## Rollback Plan

If you need to roll back, pg-tide does not remove any data. Kafka Connect
can continue processing in parallel until confidence is established.

## Next Steps

- [Bidirectional Flow: Lake to Application](../ducklake-reverse/01-reverse-relay.md)
- [Operations Runbooks](../../operations/)
