# Trino Quick-Start with pg-tide + DuckLake

Query pg-tide's DuckLake-managed Parquet files from [Trino](https://trino.io) using Trino's Hive or Iceberg connector on top of the DuckLake PostgreSQL catalog.

---

## Prerequisites

- Trino 452+
- A running pg-tide relay with DuckLake sink configured
- Parquet files accessible from the Trino cluster (local, S3, GCS, or ADLS)
- `pip install trino psycopg2-binary` (for the Python helper scripts below)

---

## Architecture Overview

```
PostgreSQL
  └── ducklake_* catalog tables
        └── file_path → s3://bucket/events/snap-42-0.parquet

Trino ──reads schema from──▶ DuckLake catalog (via Hive Metastore or direct)
       ──reads data from──▶  s3://bucket/events/*.parquet
```

---

## Step 1 — Configure Trino Hive Connector

In your Trino `etc/catalog/ducklake.properties`:

```properties
connector.name=hive
hive.metastore=file
hive.metastore.catalog.dir=/var/trino/data/lake

# For S3 backend:
hive.s3.aws-access-key=<AWS_ACCESS_KEY>
hive.s3.aws-secret-key=<AWS_SECRET_KEY>
hive.s3.region=us-east-1
```

---

## Step 2 — Register DuckLake Tables in Trino

Use a Python script to sync the DuckLake catalog into Trino's Hive Metastore via HiveQL `CREATE TABLE AS SELECT` or by adding partitions.

```python
import psycopg2
import trino

CATALOG_URL = "host=localhost dbname=mydb user=myuser"
TRINO_HOST = "localhost"
TRINO_PORT = 8080

def sync_table_to_trino(table_name: str):
    # 1. Get Parquet file paths from DuckLake catalog
    with psycopg2.connect(CATALOG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT df.file_path, df.file_size_bytes, df.file_record_count
                FROM ducklake_data_file df
                JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
                JOIN ducklake_table t ON s.table_id = t.table_id
                WHERE t.table_name = %s
                ORDER BY s.snapshot_id DESC
            """, (table_name,))
            files = cur.fetchall()

    if not files:
        print(f"No files found for table {table_name!r}")
        return

    # 2. Create or replace external table in Trino
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="trino",
        catalog="ducklake",
        schema="events",
    )
    cur = conn.cursor()

    # Build location from first file (strip filename, use directory)
    location = "/".join(files[0][0].split("/")[:-1])

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS events.{table_name} (
            _dedup_key VARCHAR,
            _subject   VARCHAR,
            _op        VARCHAR,
            _outbox_id BIGINT,
            data       VARCHAR
        )
        WITH (
            format = 'PARQUET',
            external_location = '{location}'
        )
    """)
    print(f"Table {table_name!r} registered in Trino at {location!r}")
    conn.close()
```

---

## Step 3 — Query with Trino SQL

Once registered, query your DuckLake tables from Trino directly:

```sql
-- Current data
SELECT * FROM ducklake.events.orders LIMIT 100;

-- Aggregate
SELECT
    JSON_EXTRACT_SCALAR(data, '$.status') AS status,
    COUNT(*) AS count
FROM ducklake.events.orders
GROUP BY 1
ORDER BY 2 DESC;
```

---

## Step 4 — Time Travel via Versioned View

Create a versioned external table pointing to a specific historical snapshot:

```python
def register_snapshot(table_name: str, snapshot_id: int):
    with psycopg2.connect(CATALOG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_path FROM ducklake_data_file
                WHERE snapshot_id = %s
                  AND table_id = (SELECT table_id FROM ducklake_table WHERE table_name = %s)
            """, (snapshot_id, table_name))
            paths = [r[0] for r in cur.fetchall()]

    if not paths:
        return

    location = "/".join(paths[0].split("/")[:-1])
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="trino",
                               catalog="ducklake", schema="events")
    cur = conn.cursor()
    cur.execute(f"""
        CREATE OR REPLACE VIEW events.{table_name}_v{snapshot_id} AS
        SELECT * FROM ducklake.events.{table_name}
    """)
    conn.close()
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
- [Spark Quick-Start](spark.md)
- [Pandas Quick-Start](pandas.md)
