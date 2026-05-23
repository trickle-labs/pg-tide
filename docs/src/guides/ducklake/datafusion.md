# DataFusion Quick-Start with pg-tide + DuckLake

[Apache DataFusion](https://datafusion.apache.org) is a fast, embeddable query engine written in Rust. It natively reads Parquet files and integrates well with DuckLake's PostgreSQL-backed catalog.

---

## Prerequisites

- A running pg-tide relay with DuckLake sink configured
- Python 3.10+
- `pip install datafusion psycopg2-binary pyarrow`

---

## Connecting DataFusion to Your DuckLake Lake

### 1. Locate the Parquet files via the DuckLake catalog

The DuckLake catalog is stored in your PostgreSQL instance. Query it to get the list of Parquet file paths for a given table and snapshot.

```python
import psycopg2

def get_parquet_paths(conn_str: str, table_name: str) -> list[str]:
    """Fetch the Parquet file paths for the latest snapshot of a DuckLake table."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT df.file_path
                FROM ducklake_data_file df
                JOIN ducklake_snapshot s ON df.snapshot_id = s.snapshot_id
                WHERE s.table_id = (
                    SELECT table_id FROM ducklake_table WHERE table_name = %s
                )
                ORDER BY s.snapshot_id DESC
                LIMIT 100
            """, (table_name,))
            return [row[0] for row in cur.fetchall()]
```

### 2. Register and query with DataFusion

```python
from datafusion import SessionContext

ctx = SessionContext()

pg_conn = "host=localhost dbname=mydb user=myuser password=secret"
paths = get_parquet_paths(pg_conn, "orders")

if paths:
    ctx.register_parquet("orders", paths[0])  # single file, or use a glob
    result = ctx.sql("SELECT COUNT(*) AS total FROM orders").collect()
    print(result)
```

### 3. Time travel — query a historical snapshot

```python
def get_parquet_paths_at(conn_str: str, table_name: str, snapshot_id: int) -> list[str]:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT df.file_path
                FROM ducklake_data_file df
                WHERE df.snapshot_id = %s
                  AND df.table_id = (
                    SELECT table_id FROM ducklake_table WHERE table_name = %s
                  )
            """, (snapshot_id, table_name))
            return [row[0] for row in cur.fetchall()]

old_paths = get_parquet_paths_at(pg_conn, "orders", snapshot_id=5)
ctx.register_parquet("orders_v5", old_paths[0])
ctx.sql("SELECT SUM(amount) FROM orders_v5").collect()
```

---

## pg-tide Relay Configuration

```toml
[pipeline.orders_to_lake]
source = { type = "pg_outbox", outbox = "orders" }

[pipeline.orders_to_lake.sink]
type            = "ducklake"
catalog_connection = "postgres://user:pass@localhost/mydb"
storage_provider   = "local"
root               = "/var/data/lake"
schema_change_policy = "evolve"
```

---

## End-to-End Example

```python
import asyncio
import psycopg2
from datafusion import SessionContext

CATALOG_URL = "host=localhost dbname=mydb user=myuser"

def latest_paths(table: str) -> list[str]:
    with psycopg2.connect(CATALOG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_path FROM ducklake_data_file
                WHERE table_id = (SELECT table_id FROM ducklake_table WHERE table_name = %s)
                ORDER BY snapshot_id DESC LIMIT 50
            """, (table,))
            return [r[0] for r in cur.fetchall()]

ctx = SessionContext()
paths = latest_paths("orders")
for i, p in enumerate(paths):
    ctx.register_parquet(f"part_{i}", p)

# Union all parts
union_sql = " UNION ALL ".join(f"SELECT * FROM part_{i}" for i in range(len(paths)))
df = ctx.sql(f"SELECT * FROM ({union_sql}) ORDER BY _outbox_id").collect()
print(f"Total rows: {sum(len(batch) for batch in df)}")
```

---

## Further Reading

- [DuckLake Ecosystem Compatibility Matrix](ecosystem-compatibility.md)
- [pandas Quick-Start](pandas.md)
- [Spark Quick-Start](spark.md)
