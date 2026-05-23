# Spark Quick-Start with pg-tide + DuckLake

Read pg-tide's DuckLake-managed Parquet files directly from Apache Spark using Spark's native Parquet connector and the DuckLake PostgreSQL catalog.

---

## Prerequisites

- Apache Spark 3.5+
- Python 3.10+ (PySpark)
- `pip install pyspark psycopg2-binary`
- A running pg-tide relay with DuckLake sink configured

---

## Step 1 — Resolve Parquet Paths from the DuckLake Catalog

The DuckLake catalog is a set of regular PostgreSQL tables. Query them to find the Parquet files for the latest (or a historical) snapshot.

```python
import psycopg2

def get_snapshot_paths(conn_str: str, table_name: str, snapshot_id: int = None) -> list[str]:
    """
    Return the Parquet file paths for a given DuckLake table.
    If snapshot_id is None, returns the most recent snapshot.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            if snapshot_id is None:
                cur.execute("""
                    SELECT df.file_path
                    FROM ducklake_data_file df
                    JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
                    WHERE s.table_id = (
                        SELECT table_id FROM ducklake_table WHERE table_name = %s
                    )
                    ORDER BY s.snapshot_id DESC
                """, (table_name,))
            else:
                cur.execute("""
                    SELECT df.file_path
                    FROM ducklake_data_file df
                    WHERE df.snapshot_id = %s
                      AND df.table_id = (
                        SELECT table_id FROM ducklake_table WHERE table_name = %s
                      )
                """, (snapshot_id, table_name))
            return [row[0] for row in cur.fetchall()]
```

---

## Step 2 — Read with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pg-tide DuckLake Reader") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .getOrCreate()

PG_CONN = "host=localhost dbname=mydb user=myuser password=secret"

# Get paths for the latest snapshot of the "orders" table
paths = get_snapshot_paths(PG_CONN, "orders")

if not paths:
    raise RuntimeError("No Parquet files found for table 'orders'")

df = spark.read.parquet(*paths)
df.printSchema()
df.show(10)
```

---

## Step 3 — Time Travel

Query a specific historical snapshot by passing a `snapshot_id`.

```python
# Fetch snapshot list to find the version you want
with psycopg2.connect(PG_CONN) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.snapshot_id, s.snapshot_timestamp
            FROM ducklake_snapshot s
            JOIN ducklake_table t ON s.table_id = t.table_id
            WHERE t.table_name = 'orders'
            ORDER BY s.snapshot_id
        """)
        snapshots = cur.fetchall()
        for sid, ts in snapshots:
            print(f"  snapshot {sid}: {ts}")

# Read snapshot 5
historic_paths = get_snapshot_paths(PG_CONN, "orders", snapshot_id=5)
historic_df = spark.read.parquet(*historic_paths)
historic_df.createOrReplaceTempView("orders_v5")
spark.sql("SELECT COUNT(*), SUM(CAST(data->>'amount' AS DOUBLE)) FROM orders_v5").show()
```

---

## Step 4 — Streaming with Spark Structured Streaming

Poll for new DuckLake snapshots and process them incrementally.

```python
import time

last_snapshot_id = 0

while True:
    with psycopg2.connect(PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.snapshot_id, df.file_path
                FROM ducklake_snapshot s
                JOIN ducklake_data_file df ON df.snapshot_id = s.snapshot_id
                JOIN ducklake_table t ON s.table_id = t.table_id
                WHERE t.table_name = 'orders'
                  AND s.snapshot_id > %s
                ORDER BY s.snapshot_id
            """, (last_snapshot_id,))
            rows = cur.fetchall()

    if rows:
        new_paths = [r[1] for r in rows]
        last_snapshot_id = rows[-1][0]

        micro_batch = spark.read.parquet(*new_paths)
        # process micro_batch …
        print(f"Processed {micro_batch.count()} new rows from {len(new_paths)} file(s)")

    time.sleep(5)
```

---

## pg-tide Relay Configuration

```toml
[pipeline.orders_to_lake]
source = { type = "pg_outbox", outbox = "orders" }

[pipeline.orders_to_lake.sink]
type                 = "ducklake"
catalog_connection   = "postgres://user:pass@localhost/mydb"
storage_provider     = "s3"
bucket               = "my-data-lake"
prefix               = "events/"
schema_change_policy = "evolve"
```

---

## Further Reading

- [DuckLake Ecosystem Compatibility Matrix](ecosystem-compatibility.md)
- [DataFusion Quick-Start](datafusion.md)
- [Trino Quick-Start](trino.md)
