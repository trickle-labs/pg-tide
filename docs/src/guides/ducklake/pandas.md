# Pandas Quick-Start with pg-tide + DuckLake

Read pg-tide's DuckLake-managed Parquet files into [pandas](https://pandas.pydata.org) DataFrames for data analysis, exploration, and lightweight ETL.

---

## Prerequisites

- Python 3.10+
- `pip install pandas pyarrow duckdb psycopg2-binary`
- A running pg-tide relay with DuckLake sink configured

---

## Option A — Via DuckDB (recommended)

The easiest path is to use DuckDB's native DuckLake extension. DuckDB embeds directly in Python and handles the catalog + Parquet reading transparently.

```python
import duckdb
import pandas as pd

# Connect to DuckDB and attach the DuckLake catalog from PostgreSQL
con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake;")
con.execute("ATTACH 'ducklake:postgres://user:pass@localhost/mydb' AS lake")

# Current data as a DataFrame
df: pd.DataFrame = con.execute("SELECT * FROM lake.orders").df()
print(df.dtypes)
print(df.head())
```

### Time Travel

```python
# As of a specific time
df_historic = con.execute("""
    SELECT *
    FROM lake.orders
    AT (TIMESTAMP => '2024-06-01 00:00:00'::TIMESTAMPTZ)
""").df()

# As of a specific snapshot version
df_v5 = con.execute("SELECT * FROM lake.orders AT (VERSION => 5)").df()
```

### Aggregation

```python
# Parse the `data` JSON column
import json

df["data_parsed"] = df["data"].apply(json.loads)
statuses = df["data_parsed"].apply(lambda d: d.get("status", "unknown"))
print(statuses.value_counts())
```

---

## Option B — Via PyArrow + psycopg2 (no DuckDB dependency)

Use PyArrow's Parquet reader directly, resolving file paths from the DuckLake catalog manually.

```python
import psycopg2
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

CATALOG_URL = "host=localhost dbname=mydb user=myuser password=secret"

def read_latest_snapshot(table_name: str) -> pd.DataFrame:
    """Read the latest snapshot of a DuckLake table into a pandas DataFrame."""
    with psycopg2.connect(CATALOG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT df.file_path
                FROM ducklake_data_file df
                JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
                JOIN ducklake_table t ON s.table_id = t.table_id
                WHERE t.table_name = %s
                ORDER BY s.snapshot_id DESC
            """, (table_name,))
            paths = [row[0] for row in cur.fetchall()]

    if not paths:
        raise ValueError(f"No Parquet files found for table {table_name!r}")

    tables = [pq.read_table(p) for p in paths]
    combined = pa.concat_tables(tables)
    return combined.to_pandas()


df = read_latest_snapshot("orders")
print(f"Loaded {len(df):,} rows")
print(df.head())
```

---

## Option C — Incremental Processing with Snapshot Polling

Poll for new DuckLake snapshots and process them incrementally — useful for dashboards or near-real-time analytics.

```python
import psycopg2
import pyarrow.parquet as pq
import pandas as pd
import time

CATALOG_URL = "host=localhost dbname=mydb user=myuser"

def get_new_files(conn, table_name: str, since_snapshot: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.snapshot_id, df.file_path
            FROM ducklake_snapshot s
            JOIN ducklake_data_file df ON df.snapshot_id = s.snapshot_id
            JOIN ducklake_table t ON s.table_id = t.table_id
            WHERE t.table_name = %s
              AND s.snapshot_id > %s
            ORDER BY s.snapshot_id
        """, (table_name, since_snapshot))
        return cur.fetchall()

last_snapshot = 0

with psycopg2.connect(CATALOG_URL) as conn:
    while True:
        rows = get_new_files(conn, "orders", last_snapshot)

        if rows:
            new_paths = [r[1] for r in rows]
            last_snapshot = rows[-1][0]

            frames = [pq.read_table(p).to_pandas() for p in new_paths]
            micro_batch = pd.concat(frames, ignore_index=True)
            print(f"  New rows: {len(micro_batch)}")

            # … process micro_batch …

        time.sleep(5)
```

---

## pg-tide Relay Configuration

```toml
[pipeline.orders_to_analytics]
source = { type = "pg_outbox", outbox = "orders" }

[pipeline.orders_to_analytics.sink]
type                 = "ducklake"
catalog_connection   = "postgres://user:pass@localhost/mydb"
storage_provider     = "local"
root                 = "/var/data/lake"
schema_change_policy = "evolve"
```

---

## Further Reading

- [DuckLake Ecosystem Compatibility Matrix](ecosystem-compatibility.md)
- [DataFusion Quick-Start](datafusion.md)
- [Spark Quick-Start](spark.md)
- [Trino Quick-Start](trino.md)
