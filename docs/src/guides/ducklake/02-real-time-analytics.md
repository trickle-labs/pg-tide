# Real-Time Analytics with DuckDB

This tutorial shows you how to run real-time analytical queries against a
DuckLake populated by pg-tide, using DuckDB's columnar engine for fast
aggregations over event streams.

## What You'll Learn

- How DuckLake's data inlining gives you sub-millisecond write latency for streaming events
- How to write analytical queries that always see the latest events
- How to use DuckDB's window functions and aggregations on a live event stream

## Prerequisites

- Completed [Tutorial 1](01-from-transaction-to-data-lake.md)
- The Docker Compose stack running

## Architecture

```
PostgreSQL (pg_tide outbox)
        │
        │  poll batch (10 rows/s)
        ▼
pg-tide relay
        │
        │  small batches  ──► ducklake_inlined_data_* (catalog, instant)
        │  large batches  ──► Parquet files (MinIO) + catalog metadata
        ▼
DuckLake catalog (PostgreSQL tables)
        │
        │  ATTACH
        ▼
DuckDB (queries spanning inlined rows + Parquet files transparently)
```

## Writing Analytical Queries

### Live order totals

```sql
SELECT
    date_trunc('hour', created_at) AS hour,
    status,
    COUNT(*)                       AS order_count,
    SUM(amount)                    AS total_amount,
    AVG(amount)                    AS avg_amount
FROM lake.pgtide.orders
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

### Customer segmentation

```sql
SELECT
    customer,
    COUNT(DISTINCT order_id)   AS total_orders,
    SUM(amount)                AS lifetime_value,
    MAX(created_at)            AS last_order_at,
    DATEDIFF('day', MIN(created_at), MAX(created_at)) AS active_days
FROM lake.pgtide.orders
GROUP BY customer
HAVING COUNT(*) > 5
ORDER BY lifetime_value DESC
LIMIT 20;
```

### Rolling 5-minute throughput

```sql
SELECT
    date_trunc('minute', created_at) AS minute,
    COUNT(*) AS events,
    SUM(COUNT(*)) OVER (
        ORDER BY date_trunc('minute', created_at)
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5min
FROM lake.pgtide.orders
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;
```

## Using Time-Travel for Point-in-Time Analysis

DuckLake records every version of the table. You can query historical states:

```sql
-- What did the lake look like after the first batch was delivered?
SELECT COUNT(*), SUM(amount)
FROM lake.pgtide.orders AT (VERSION => 1);

-- Compare two versions.
SELECT
    (SELECT COUNT(*) FROM lake.pgtide.orders AT (VERSION => 5))  AS v5_count,
    (SELECT COUNT(*) FROM lake.pgtide.orders AT (VERSION => 10)) AS v10_count;
```

Use `pg-tide ducklake offset-map` to translate consumer offsets to snapshot IDs:

```bash
pg-tide ducklake offset-map \
  --pipeline orders-ducklake \
  --postgres-url postgres://pgtide:pgtide@localhost:5432/pgtide
```

## Performance Tips

- DuckLake's columnar Parquet storage makes `SUM` / `AVG` / `COUNT` queries up to 100× faster than PostgreSQL on the same data.
- For streaming workloads with many small batches, set `inline_row_limit = 10` (default) to keep events in PostgreSQL until enough accumulate for efficient Parquet encoding.
- Run `pg-tide ducklake checkpoint` during low-traffic windows to consolidate many small Parquet files into fewer large ones.

## Next Steps

- [Multi-Tenant Data Lake with Row-Level Security](03-multi-tenant.md)
- [Event Sourcing with DuckLake as the Event Store](04-event-sourcing.md)
