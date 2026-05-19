# From Transaction to Data Lake in 5 Minutes

This tutorial shows you how to go from a fresh PostgreSQL database to a
queryable DuckLake event archive in under five minutes, using pg-tide as
the streaming transport.

## What You'll Build

- A PostgreSQL database with the `pg_tide` extension installed
- An **orders outbox** that captures business events inside transactions
- A pg-tide relay streaming events to **DuckLake** (Parquet files on object storage with a PostgreSQL catalog)
- A DuckDB session that queries the live lake with full time-travel support

## Prerequisites

- Docker and Docker Compose
- 5 minutes

## Step 1: Start the Stack

```bash
cd examples/ducklake
docker compose up -d
```

This starts PostgreSQL 18 with pg_tide, MinIO for object storage, the pg-tide relay, and Grafana. The relay begins streaming immediately.

## Step 2: Publish Events

```bash
docker compose run seed
```

This publishes 1 000 synthetic order events atomically. Each `tide.outbox_publish()` call captures the event inside the same PostgreSQL transaction that writes your business data — zero risk of silent event loss.

## Step 3: Watch the Relay

```bash
docker compose logs -f relay
```

You'll see the relay polling the outbox, batching messages, and writing DuckLake snapshots. Each batch creates a new `ducklake_snapshot` entry and either writes a Parquet file or inlines rows directly in the catalog (for small batches).

## Step 4: Query from DuckDB

```bash
docker compose exec duckdb duckdb
```

Inside DuckDB:

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres:host=postgres user=pgtide password=pgtide dbname=pgtide'
     AS lake (DATA_PATH 's3://pg-tide-lake/orders/');

-- Query the live lake.
SELECT status, COUNT(*), SUM(amount)
FROM lake.pgtide.orders
GROUP BY status
ORDER BY status;

-- Time-travel: see the lake as it was after snapshot 1.
SELECT COUNT(*) FROM lake.pgtide.orders AT (VERSION => 1);
```

## Step 5: Inspect with pg-tide CLI

```bash
docker compose exec relay pg-tide ducklake snapshots \
  --pipeline orders-ducklake \
  --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide
```

## What Just Happened?

1. Your application called `tide.outbox_publish()` inside a PostgreSQL transaction.
2. The pg-tide relay polled the outbox, decoded the events, and wrote a Parquet file to MinIO.
3. In the same PostgreSQL transaction, the relay updated `ducklake_snapshot`, `ducklake_data_file`, and `tide.ducklake_offset_map`.
4. DuckDB's `ducklake` extension reads the catalog tables and serves queries directly — no ETL tool, no glue code.

## Next Steps

- [Real-Time Analytics with DuckDB](02-real-time-analytics.md)
- [Multi-Tenant Data Lake with Row-Level Security](03-multi-tenant.md)
- [Event Sourcing with DuckLake](04-event-sourcing.md)
