# DuckLake Ecosystem Compatibility

pg-tide's DuckLake sink writes Parquet files alongside a DuckLake v1.0 catalog (stored in PostgreSQL). Any engine that can attach to a DuckLake catalog can read the resulting lake without any translation layer.

---

## Compatibility Matrix

| Engine | Version Tested | Read | Write | Time Travel | Schema Evolution |
|--------|---------------|------|-------|-------------|-----------------|
| [DuckDB](https://duckdb.org) | ≥ 1.2 | ✅ | ✅ | ✅ | ✅ |
| [DataFusion](https://datafusion.apache.org) | ≥ 44 | ✅ | — | ✅ via snapshot filter | ✅ |
| [Apache Spark](https://spark.apache.org) | 3.5 | ✅ | — | ✅ via `VERSION AS OF` | ✅ |
| [Trino](https://trino.io) | ≥ 452 | ✅ | — | ✅ via `FOR VERSION AS OF` | ✅ |
| [pandas](https://pandas.pydata.org) + DuckDB | latest | ✅ | — | ✅ via DuckDB | ✅ |

---

## Time-Travel Queries

### DuckDB

DuckDB has native DuckLake support via the `ducklake` extension.

```sql
ATTACH 'ducklake:postgres://user:pass@host/mydb' AS lake;

-- Current snapshot
SELECT * FROM lake.events LIMIT 100;

-- Snapshot at a specific time
SELECT * FROM lake.events AT (TIMESTAMP => '2024-06-01 12:00:00'::TIMESTAMPTZ);

-- Specific snapshot ID
SELECT * FROM lake.events AT (VERSION => 42);
```

### DataFusion (via deltalake or parquet direct read)

DataFusion reads DuckLake Parquet files directly. Use the `snapshot_id` from the catalog to select the right files.

```python
import datafusion
from datafusion import SessionContext
import psycopg2

ctx = SessionContext()

# Query the DuckLake catalog for Parquet file paths at a specific snapshot
with psycopg2.connect("host=localhost dbname=mydb user=myuser") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT df.file_path
            FROM ducklake_data_file df
            JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
            WHERE s.table_id = (
                SELECT table_id FROM ducklake_table WHERE table_name = 'events'
            )
            ORDER BY s.snapshot_id DESC
            LIMIT 1
        """)
        paths = [row[0] for row in cur.fetchall()]

for path in paths:
    ctx.register_parquet("events_snapshot", path)

result = ctx.sql("SELECT * FROM events_snapshot").collect()
```

### Apache Spark

Read the Parquet files registered in the DuckLake catalog using Spark's Parquet reader.

```python
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder \
    .appName("pg-tide DuckLake Reader") \
    .getOrCreate()

# Fetch file list from DuckLake catalog
with psycopg2.connect("host=localhost dbname=mydb user=myuser") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT df.file_path
            FROM ducklake_data_file df
            JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
            WHERE s.table_id = (
                SELECT table_id FROM ducklake_table WHERE table_name = 'events'
            )
            ORDER BY s.snapshot_id DESC
            LIMIT 1
        """)
        paths = [row[0] for row in cur.fetchall()]

df = spark.read.parquet(*paths)
df.createOrReplaceTempView("events")
spark.sql("SELECT * FROM events LIMIT 100").show()
```

### Trino

Configure Trino's Hive/Iceberg connector to point at the Parquet files registered in DuckLake.

```sql
-- Using Trino's SQL connector to query a DuckLake-registered path
SELECT *
FROM hive.default.events
FOR VERSION AS OF 42;
```

### pandas + DuckDB

```python
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake;")
con.execute("ATTACH 'ducklake:postgres://user:pass@host/mydb' AS lake")

df: pd.DataFrame = con.execute(
    "SELECT * FROM lake.events AT (TIMESTAMP => '2024-06-01'::TIMESTAMPTZ)"
).df()
print(df.head())
```

---

## Storage Backend Compatibility

| Backend | DuckDB | DataFusion | Spark | Trino | pandas+DuckDB |
|---------|--------|-----------|-------|-------|--------------|
| Local filesystem | ✅ | ✅ | ✅ | ✅ | ✅ |
| Amazon S3 | ✅ | ✅ | ✅ | ✅ | ✅ |
| Google Cloud Storage | ✅ | ✅ | ✅ | ✅ | ✅ |
| Azure Blob Storage | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Related Guides

- [DataFusion Quick-Start](datafusion.md)
- [Spark Quick-Start](spark.md)
- [Trino Quick-Start](trino.md)
- [Pandas Quick-Start](pandas.md)
