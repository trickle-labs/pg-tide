# Multi-Tenant Data Lake with Row-Level Security

This tutorial shows how to build a multi-tenant DuckLake where each tenant's
events are isolated using PostgreSQL Row-Level Security (RLS) and pg-tide's
tenant-aware relay groups.

## What You'll Build

- A shared `orders` outbox with a `tenant_id` discriminator column
- Per-tenant RLS policies that prevent cross-tenant data access
- A DuckLake sink that partitions by tenant for query efficiency
- Per-tenant consumer lag monitoring

## Prerequisites

- Completed [Tutorial 1](01-from-transaction-to-data-lake.md)
- Basic familiarity with PostgreSQL RLS

## Step 1: Create a Tenant-Aware Outbox

```sql
-- Create the outbox with a tenant discriminator.
SELECT tide.outbox_create(
    'orders_mt',
    'payload JSONB NOT NULL, tenant_id TEXT NOT NULL'
);
```

## Step 2: Set Up Row-Level Security

```sql
-- Enable RLS on the outbox messages table.
ALTER TABLE tide.outbox_messages_orders_mt ENABLE ROW LEVEL SECURITY;

-- Policy: relay role sees everything (for the pg-tide relay worker).
CREATE POLICY relay_all ON tide.outbox_messages_orders_mt
    FOR ALL TO pg_tide_relay USING (true);

-- Policy: tenant application roles see only their rows.
CREATE POLICY tenant_acme ON tide.outbox_messages_orders_mt
    FOR SELECT TO app_acme USING (payload->>'tenant_id' = 'acme');

CREATE POLICY tenant_globex ON tide.outbox_messages_orders_mt
    FOR SELECT TO app_globex USING (payload->>'tenant_id' = 'globex');
```

## Step 3: Configure a DuckLake Sink with Bucket Partitioning

```sql
SELECT tide.relay_set_outbox_v2(jsonb_build_object(
    'name',       'orders-mt-ducklake',
    'outbox',     'orders_mt',
    'sink_type',  'ducklake',
    'batch_size', 50,
    'enabled',    true,
    'config', jsonb_build_object(
        'data_path',      's3://pg-tide-lake/orders-mt/',
        'namespace',      'pgtide',
        'table_template', 'orders_mt',
        'catalog_schema', 'ducklake',
        'partition',      'bucket:8'   -- bucket-partition by _subject
    )
));
```

## Step 4: Publish Tenant Events

```sql
-- Tenant ACME publishes an order.
PERFORM tide.outbox_publish('orders_mt',
    '{"order_id": 1, "tenant_id": "acme", "amount": 199.99}'
);

-- Tenant Globex publishes an order.
PERFORM tide.outbox_publish('orders_mt',
    '{"order_id": 2, "tenant_id": "globex", "amount": 89.50}'
);
```

## Step 5: Query with Tenant Isolation in DuckDB

```sql
-- All events (admin view).
SELECT tenant_id, COUNT(*), SUM(amount)
FROM lake.pgtide.orders_mt
GROUP BY tenant_id;

-- Tenant ACME's view (application-level filter).
SELECT *
FROM lake.pgtide.orders_mt
WHERE payload->>'tenant_id' = 'acme'
ORDER BY created_at DESC
LIMIT 10;
```

## Monitoring Per-Tenant Lag

```bash
pg-tide status \
  --postgres-url postgres://pgtide:pgtide@localhost:5432/pgtide
```

Look for the `orders-mt-ducklake` pipeline; the `Consumer Lag` column shows
how many events are pending delivery to the lake.

## Next Steps

- [Event Sourcing with DuckLake as the Event Store](04-event-sourcing.md)
- [Migrating from Kafka Connect](05-migrating-from-kafka-connect.md)
