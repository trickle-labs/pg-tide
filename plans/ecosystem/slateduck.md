# pg-tide × SlateDuck: Native Integration Plan

> **Status:** Design  
> **Date:** 2026-05-23  
> **Relates to:** [plans/ecosystem/ducklake.md](ducklake.md) (PostgreSQL-backed DuckLake),
> [trickle-labs/slateduck](https://github.com/trickle-labs/slateduck),
> [slatedb-ducklake design doc](https://github.com/trickle-labs/slateduck/blob/master/slatedb-ducklake.md)

---

## 1. Executive Summary

[SlateDuck](https://github.com/trickle-labs/slateduck) is a project building a DuckLake catalog
backed by SlateDB — an embedded LSM key-value store that lives entirely in object storage (S3,
GCS, Azure Blob). The result is a "lakehouse in a bucket": both the catalog metadata and the
Parquet data files live in the same S3 bucket, with no PostgreSQL server, no database to
provision, patch, or back up.

SlateDuck's production implementation (Strategy B) exposes a **PostgreSQL wire-protocol
sidecar** that DuckDB clients connect to using DuckDB's standard `postgres` extension. Because
the pg-tide relay already has DuckLake source and sink implementations driven over
`tokio-postgres`, it should theoretically connect to SlateDuck as a drop-in replacement for a
real PostgreSQL catalog.

**It cannot — yet.** A thorough audit of the relay's DuckLake SQL against SlateDuck's bounded
SQL subset reveals eleven classes of incompatible query patterns. This document:

1. Catalogs every incompatibility with its root cause and resolution;
2. Describes a clean `SlateDuckSink` / `SlateDuckSource` architecture that is compatible with
   SlateDuck's bounded SQL subset;
3. Provides a phased implementation plan that delivers a full-featured, best-in-class pg-tide
   integration for SlateDuck.

The reward is significant: when complete, pg-tide becomes the first production-grade event
streaming system with native SlateDuck support — giving users a **zero-infrastructure path from
a PostgreSQL transaction to a queryable, time-traveling data lake in S3** with no separate
database server.

---

## 2. Background

### 2.1 SlateDuck vs. PostgreSQL-backed DuckLake

Both are DuckLake catalogs. The difference is where the catalog lives:

| Aspect | DuckLake-on-PostgreSQL | SlateDuck |
|---|---|---|
| Catalog store | PostgreSQL tables | SlateDB in S3 |
| Infrastructure | Managed RDS or bare PG | No server — just an S3 bucket |
| Protocol to relay | Native `tokio-postgres` | PostgreSQL wire over TCP to a stateless sidecar |
| ID allocation | `nextval(sequence)` | Internal KV counters, writer supplies explicit IDs |
| DDL | `CREATE TABLE / SEQUENCE` | SlateDuck initializes its own catalog on first open |
| NOTIFY | `pg_notify(...)` | Not supported |
| SQL surface | Full PostgreSQL | Bounded spec subset (≈30 statement families) |
| Exactly-once with outbox | Single PG transaction | Requires idempotency via `ducklake_metadata` |
| Cost | RDS hourly + storage | S3 PUT/GET only |
| Best for | Interactive queries, many concurrent analysts | Serverless, batch ingest, "lakehouse in a bucket" |

The relay's existing `DuckLakeSink` and `DuckLakeSource` were written for the PostgreSQL path.
This plan covers building native SlateDuck variants.

### 2.2 SlateDuck's Bounded SQL Subset (Strategy B)

SlateDuck's PG-wire sidecar (Phase 4 in the SlateDuck roadmap) deliberately supports only the
SQL statement families that the DuckLake specification actually issues. The complete set from
the SlateDuck design doc:

**Read operations (6 families):**
- `SELECT max(snapshot_id) FROM ducklake_snapshot WHERE ...` — current snapshot lookup
- `SELECT ... FROM ducklake_{table} WHERE [parent_id = ?] AND begin_snapshot <= ? AND (end_snapshot IS NULL OR ? < end_snapshot)` — MVCC-filtered catalog scans
- `SELECT data.path, del.path FROM ducklake_data_file LEFT JOIN ducklake_delete_file USING (data_file_id) WHERE ...` — the only supported JOIN
- Pruning queries against `ducklake_file_column_stats`
- Handshake / `pg_catalog` introspection (`SELECT current_schema()`, `SHOW server_version`, etc.)

**Write operations (~15 families):**
- `INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, ...) VALUES (...)`
- `INSERT INTO ducklake_{schema|table|column|data_file|delete_file|...} VALUES (...)`
- `INSERT INTO ducklake_file_column_stats (...) VALUES (...)`
- `UPDATE ducklake_table_stats SET record_count = record_count + ? WHERE table_id = ?`
- `UPDATE ducklake_{table} SET end_snapshot = ? WHERE [id_col] = ? AND end_snapshot IS NULL`
- Dynamic inlined-data DDL/DML: `CREATE TABLE ducklake_inlined_*`, `INSERT` into inlined tables
- `BEGIN` / `COMMIT` / `ROLLBACK`

**Notably absent:** `nextval()`, multi-table `INNER JOIN` beyond the one LEFT JOIN on file IDs,
`ON CONFLICT`, `CREATE SEQUENCE`, `CREATE TABLE` for catalog tables, subqueries in `INSERT
VALUES`, `pg_notify()`, CTEs with `DELETE`, `RETURNING`, `LIMIT` with parameters (unconfirmed),
PostgreSQL-specific system functions.

### 2.3 A Critical Pre-Existing Issue: Non-Spec Catalog Schema

The relay's current `DuckLakeSink::ensure_catalog()` creates a **simplified, non-v1.0-spec
catalog schema**. The most significant deviations from the DuckLake v1.0 spec:

| What the relay creates | What the v1.0 spec says |
|---|---|
| `ducklake_snapshot(snapshot_id, table_id, schema_version, sequence_number, ...)` | Snapshots are **catalog-wide** — no `table_id`; the spec fields are `next_catalog_id` and `next_file_id` |
| `CREATE SEQUENCE ducklake_snapshot_id_seq`, etc. | ID allocation via `next_catalog_id` / `next_file_id` from the previous snapshot |
| 8 tables | 28 tables in the full v1.0 spec |
| `ducklake_snapshot_changes.table_id` references the table | Spec: changes reference an overall catalog snapshot |

This means the relay's existing DuckLake output **cannot be queried by DuckDB directly** — a
DuckDB `ATTACH` to the relay's catalog would fail on schema validation. This is the core problem
that the [ducklake.md](ducklake.md) plan addresses in its Phase 1.

**SlateDuck implements the real v1.0 spec**. Connecting the current relay to SlateDuck would
therefore fail for two independent reasons: wrong SQL syntax AND wrong table structure.

**Upgrading to the real v1.0 spec** (ducklake.md Phase 1) is a hard prerequisite for
SlateDuck support. These two efforts are aligned and the SlateDuck work can share the v1.0 spec
catalog code.

---

## 3. Gap Analysis: Full Incompatibility Audit

Every SQL pattern in the current relay `DuckLakeSink` and `DuckLakeSource` that SlateDuck
cannot execute:

### 3.1 Sequences (`nextval`)

**Locations:** `publish()` — snapshot ID allocation; Parquet path — file ID allocation;
`bootstrap_table()` — schema/table/column ID allocation; `add_column_additive()`.

**Example:**
```sql
SELECT nextval('"ducklake".ducklake_snapshot_id_seq')
SELECT nextval('"ducklake".ducklake_file_id_seq')
```

**Why incompatible:** SlateDuck has no PostgreSQL sequences. It allocates IDs internally via
KV counters (`0xFE` prefix). The DuckLake v1.0 client protocol assigns explicit IDs by reading
`next_catalog_id` and `next_file_id` from the most-recent `ducklake_snapshot` row.

**Resolution:** Read the previous snapshot to get the ID allocation range, then INSERT with
explicit IDs. Carry the consumed range forward in the new snapshot's `next_catalog_id` and
`next_file_id` fields. This is how DuckDB itself allocates IDs when talking to SlateDuck.

---

### 3.2 DDL — `CREATE SCHEMA / SEQUENCE / TABLE`

**Location:** `ensure_catalog()` — issues 14 DDL statements on first run.

**Example:**
```sql
CREATE SCHEMA IF NOT EXISTS "ducklake";
CREATE SEQUENCE IF NOT EXISTS "ducklake".ducklake_snapshot_id_seq START WITH 1;
CREATE TABLE IF NOT EXISTS "ducklake".ducklake_snapshot (...);
```

**Why incompatible:** SlateDuck's catalog tables are defined by its internal key layout and
initialized when the writer first opens the catalog. It does not accept DDL for the 28 catalog
tables. Only `CREATE TABLE ducklake_inlined_*` (dynamic inlined-data tables) is within the
bounded set.

**Resolution:** Remove `ensure_catalog()` entirely from the SlateDuck path. Replace with
`verify_catalog_ready()` — a single `SELECT value FROM ducklake_metadata WHERE key = 'version'`
to confirm the catalog is initialized and return a clear error if not.

---

### 3.3 Upsert — `ON CONFLICT DO UPDATE / DO NOTHING`

**Locations:** `bootstrap_table()` — schema and table inserts; `add_column_additive()` —
column insert; `publish()` Parquet path — `ducklake_file_column_stats` and
`ducklake_table_column_stats` upserts.

**Example:**
```sql
INSERT INTO "ducklake".ducklake_schema (schema_id, schema_name)
VALUES (nextval('...'), $1)
ON CONFLICT (schema_name) DO UPDATE SET schema_name = EXCLUDED.schema_name
RETURNING schema_id
```

**Why incompatible:** `ON CONFLICT` is a PostgreSQL extension absent from SlateDuck's bounded
write set.

**Resolution:** Explicit SELECT → conditional INSERT pattern. Since SlateDuck has a single
writer, the SELECT and INSERT are race-free. Cache results in `bootstrapped_tables` and
`column_ids` as before — the cache means we only do the round-trip once per relay process
lifetime.

---

### 3.4 `RETURNING` Clause

**Locations:** All `bootstrap_table()` inserts and `add_column_additive()`.

**Example:**
```sql
INSERT INTO "ducklake".ducklake_table (...) VALUES (...) RETURNING table_id
```

**Why incompatible:** `RETURNING` is not in SlateDuck's bounded write set.

**Resolution:** Pre-allocate IDs explicitly (from the `next_catalog_id` counter read from the
last snapshot). INSERT with the known ID. No `RETURNING` needed.

---

### 3.5 Multi-table INNER JOIN in SELECT

**Location:** `DuckLakeSource::poll()` — snapshot poll query and table ID lookup.

**Example:**
```sql
SELECT max(s.snapshot_id)
FROM ducklake_snapshot s
JOIN ducklake_table t ON t.table_id = s.table_id
JOIN ducklake_schema sc ON sc.schema_id = t.schema_id
WHERE sc.schema_name = $1 AND t.table_name = $2 AND s.snapshot_id > $3
```

**Why incompatible:** SlateDuck allows exactly one `LEFT JOIN` between `ducklake_data_file` and
`ducklake_delete_file`. Multi-table `INNER JOIN` across snapshot / table / schema tables is
outside the bounded set.

**Resolution:** Sequential round trips:
1. `SELECT schema_id FROM ducklake_schema WHERE schema_name = $1`
2. `SELECT table_id FROM ducklake_table WHERE schema_id = $1 AND table_name = $2`
3. `SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $1` (catalog-wide
   max — v1.0 snapshots are not per-table)

Cache schema_id and table_id after first lookup; only round trip 3 recurs on each poll.

---

### 3.6 Subquery in `INSERT VALUES`

**Location:** `publish_inline()` and `publish()` Parquet path — `sequence_number` derivation.

**Example:**
```sql
INSERT INTO "ducklake".ducklake_snapshot (snapshot_id, table_id, schema_version, sequence_number, author)
VALUES ($1, $2, $3,
    COALESCE((SELECT MAX(sequence_number) + 1 FROM "ducklake".ducklake_snapshot WHERE table_id = $2), 0),
    'pg-tide-relay')
```

**Why incompatible:** Subqueries in `INSERT VALUES` are outside SlateDuck's bounded set (the
`max()` subquery is only permitted in the specific current-snapshot SELECT shape).

**Resolution:** Separate `SELECT COALESCE(MAX(sequence_number) + 1, 0) FROM ...` before the
transaction, then INSERT with explicit `$N` parameters. In the v1.0 spec implementation the
`sequence_number` field moves to the snapshot's `schema_version` tracking, which is pre-computed
in `CatalogWriter.mark_schema_changed()`.

---

### 3.7 `pg_notify()`

**Locations:** `publish_inline()` and `publish()` Parquet path — fires after each snapshot
commit.

**Example:**
```sql
SELECT pg_notify('tide_ducklake_changes', $1)
```

**Why incompatible:** `pg_notify` is a PostgreSQL-specific function with no equivalent in
SlateDuck's bounded SQL set.

**Resolution:** Remove from the SlateDuck path. No NOTIFY mechanism is available. Downstream
consumers must poll `ducklake_snapshot`. Document this trade-off: SlateDuck consumers use
polling rather than push notifications.

---

### 3.8 `tide.*` Tables (`ducklake_offset_map`, `ducklake_partition_config`)

**Locations:** `write_offset_map()` and `register_partition_config()` — write to pg-tide
management tables in the `tide` schema.

**Example:**
```sql
INSERT INTO tide.ducklake_offset_map (pipeline_name, consumer_group, outbox_offset, snapshot_id)
VALUES ($1, $2, $3, $4) ON CONFLICT (...) DO NOTHING
```

**Why incompatible:** `tide.*` tables are pg-tide-specific tables that exist only in the
PostgreSQL instance hosting the pg-tide extension. SlateDuck has no knowledge of them and would
return `SQLSTATE 0A000` (feature not supported) or `42P01` (relation not found).

**Resolution:** Store relay-managed state in `ducklake_metadata` using scoped keys, which
SlateDuck fully supports:

```
-- Outbox offset → snapshot_id mapping (scope=table, scope_id=table_uuid)
key: 'pg_tide.pipeline.{pipeline_name}.offset.{outbox_offset}'
value: '{snapshot_id}'

-- Partition strategy (scope=table, scope_id=table_uuid)
key: 'pg_tide.partition.{pipeline_name}'
value: '{partition_type}'
```

The `ducklake_metadata` table is a standard DuckLake v1.0 catalog table (tag `0x01` in
SlateDuck). INSERTs into it are fully supported.

---

### 3.9 CTE with `DELETE` — DLQ Archive

**Location:** `archive_dlq_entries()` — moves aged DLQ entries from `tide.relay_dlq` to a
DuckLake archive table.

**Example:**
```sql
WITH aged AS (
    DELETE FROM tide.relay_dlq WHERE failed_at < now() - ($1 * INTERVAL '1 hour')
    RETURNING pipeline_name, dedup_key, subject, payload, error_message, failed_at
)
INSERT INTO "ducklake".dlq_archive (...) SELECT ... FROM aged
```

**Why incompatible:** CTE with `DELETE` is entirely absent from SlateDuck's SQL subset. Also
references `tide.relay_dlq` which does not exist in SlateDuck.

**Resolution:** Skip DLQ archival in the SlateDuck path entirely. The DLQ archive feature
requires co-located PostgreSQL. Document this: users who want DLQ archival should use the
PostgreSQL-backed DuckLake sink.

---

### 3.10 Complex Upsert with `CASE` Expressions

**Location:** `publish()` Parquet path — `ducklake_table_column_stats` upsert with
min/max merge logic.

**Example:**
```sql
INSERT INTO "ducklake".ducklake_table_column_stats (table_id, column_id, min_value, max_value, null_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (table_id, column_id) DO UPDATE
    SET min_value = CASE
        WHEN EXCLUDED.min_value IS NOT NULL AND (ducklake_table_column_stats.min_value IS NULL
             OR EXCLUDED.min_value < ducklake_table_column_stats.min_value)
        THEN EXCLUDED.min_value
        ELSE ducklake_table_column_stats.min_value
        END,
        ...
```

**Why incompatible:** `ON CONFLICT DO UPDATE` with `CASE` expressions is outside the bounded
set. SlateDuck supports only plain `UPDATE ducklake_table_stats SET record_count = record_count + ?`.

**Resolution:** Two-step operation:
1. `SELECT min_value, max_value, null_count FROM ducklake_table_column_stats WHERE table_id = ? AND column_id = ?`
2. Merge in Rust (compare batch stats with existing stats)
3. If row exists: `UPDATE ducklake_table_column_stats SET min_value = ?, max_value = ?, null_count = ? WHERE table_id = ? AND column_id = ?`
4. If row absent: `INSERT INTO ducklake_table_column_stats (...) VALUES (...)`

Same for `ducklake_file_column_stats`.

---

### 3.11 `isolation_level(ReadCommitted)` Transaction Mode

**Location:** `publish_inline()` and `publish()` — `build_transaction().isolation_level(ReadCommitted)`.

**Why potentially incompatible:** SlateDuck's transaction model uses `SerializableSnapshot`
isolation (as specified in its design doc). `READ COMMITTED` may not map cleanly.

**Resolution:** Use `BEGIN` / `COMMIT` without explicit isolation level, or use
`SerializableSnapshot` if SlateDuck requires it. Validate in Phase 0 integration testing.

---

### Summary Table

| # | Pattern | Severity | Resolution |
|---|---|---|---|
| 1 | `nextval(sequence)` | **Blocker** | Read `next_catalog_id` / `next_file_id` from prev snapshot |
| 2 | `CREATE SCHEMA / SEQUENCE / TABLE` (catalog DDL) | **Blocker** | Remove `ensure_catalog()`; add `verify_catalog_ready()` |
| 3 | `ON CONFLICT DO UPDATE / NOTHING` | **Blocker** | SELECT → conditional INSERT/UPDATE |
| 4 | `RETURNING` clause | **Blocker** | Pre-allocate IDs, no RETURNING needed |
| 5 | Multi-table INNER JOIN | **Blocker** | Sequential point reads (3 round trips, cached) |
| 6 | Subquery in INSERT VALUES | **Blocker** | Pre-execute subquery as separate SELECT |
| 7 | `pg_notify()` | **Blocker** | Remove; document polling model |
| 8 | `tide.*` tables | **Blocker** | Use `ducklake_metadata` scoped keys |
| 9 | CTE with DELETE | **Blocker** | Skip DLQ archival in SlateDuck path |
| 10 | Complex CASE in ON CONFLICT | **Blocker** | SELECT + merge in Rust + INSERT/UPDATE |
| 11 | `ReadCommitted` isolation level | Verify | Use SlateDuck default; test in Phase 0 |

All 10 hard blockers have clean resolutions. None require SlateDuck to be extended.

---

## 4. Architecture

### 4.1 New Crate-level Types

```
pg-tide-relay/src/
├── sink/
│   ├── ducklake.rs          (existing — PostgreSQL-backed, refactored to real v1.0 spec)
│   └── slateduck.rs         (NEW — SlateDuck-native sink)
├── source/
│   ├── ducklake.rs          (existing — PostgreSQL-backed)
│   └── slateduck.rs         (NEW — SlateDuck-native source)
└── ducklake_common/
    ├── mod.rs               (NEW — shared types: DuckLakePartition, SchemaChangePolicy, etc.)
    ├── parquet.rs           (NEW — extracted Parquet builder, shared between PG and SlateDuck)
    ├── catalog_ids.rs       (NEW — ID allocation from prev snapshot's next_catalog_id)
    └── column_stats.rs      (NEW — ColStats, compute_column_stats, str_min_max)
```

The `ducklake_common` module is extracted from the existing sink to avoid duplication. Both
`DuckLakeSink` and `SlateDuckSink` use the same Parquet builder, column statistics computation,
schema change detection, and partition logic.

### 4.2 `SlateDuckSink`

```rust
pub struct SlateDuckConfig {
    /// PG-wire endpoint of the SlateDuck sidecar
    /// e.g. "host=slateduck-writer.svc port=5432 dbname=catalog"
    pub sidecar_endpoint: String,
    /// Object storage root for Parquet data files
    pub data_path: String,
    /// DuckLake namespace (maps to ducklake_schema.schema_name)
    pub namespace: String,
    /// Table name template; `{stream_table}` replaced with message subject
    pub table_template: String,
    pub compression: DuckLakeCompression,
    /// Rows at or below this count go to the inlined-data path (default: 10)
    pub inline_row_limit: usize,
    pub on_schema_change: SchemaChangePolicy,
    pub partition: DuckLakePartition,
    /// Pipeline name for offset tracking via ducklake_metadata
    pub pipeline_name: Option<String>,
    // NOTE: `atomic_lake_writes` is NOT available — SlateDuck and the pg-tide
    // outbox are in separate systems. Use `pipeline_name` + idempotency instead.
}

pub struct SlateDuckSink {
    store: Arc<dyn ObjectStore>,
    db: tokio_postgres::Client,     // connected to SlateDuck sidecar PG-wire
    config: SlateDuckConfig,
    // Cached identifiers (populated at bootstrap, never require RETURNING)
    schema_ids: HashMap<String, i64>,
    table_ids: HashMap<(String, String), i64>,
    column_ids: HashMap<(i64, String), i64>,
    // Next-ID state read from the most recent snapshot
    next_catalog_id: i64,
    next_file_id: i64,
    last_snapshot_id: i64,
    schema_changed: bool,
}
```

Key invariant: `next_catalog_id` and `next_file_id` are always loaded from the most recent
`ducklake_snapshot` row at connection time and updated in-memory after each successful commit.
No `nextval()` calls are ever issued.

### 4.3 `SlateDuckSource`

```rust
pub struct SlateDuckSourceConfig {
    pub sidecar_endpoint: String,
    pub namespace: String,
    pub table: String,
    pub snapshot_poll_interval_ms: u64,
    pub consumer_group: String,
}

pub struct SlateDuckSource {
    config: SlateDuckSourceConfig,
    // Resolved at startup, cached forever (SlateDuck is single-writer,
    // no risk of schema_id / table_id changing without relay restart)
    schema_id: Option<i64>,
    table_id: Option<i64>,
    last_snapshot_id: i64,
}
```

### 4.4 ID Allocation Without Sequences

The DuckLake v1.0 protocol allocates IDs by reading the `next_catalog_id` and `next_file_id`
from the most recent snapshot row, then committing the new snapshot with updated counters:

```rust
// On sink startup and after each successful commit:
async fn refresh_id_state(&mut self) -> Result<(), RelayError> {
    let row = self.db.query_opt(
        "SELECT snapshot_id, next_catalog_id, next_file_id, schema_version \
         FROM ducklake_snapshot \
         ORDER BY snapshot_id DESC LIMIT 1",
        &[],
    ).await?;

    match row {
        Some(r) => {
            self.last_snapshot_id = r.get("snapshot_id");
            self.next_catalog_id  = r.get("next_catalog_id");
            self.next_file_id     = r.get("next_file_id");
            self.current_schema_version = r.get("schema_version");
        }
        None => {
            // Fresh catalog — counters start at 1
            self.last_snapshot_id = 0;
            self.next_catalog_id  = 1;
            self.next_file_id     = 1;
            self.current_schema_version = 0;
        }
    }
    Ok(())
}

// Allocate one or more catalog IDs (schemas, tables, columns)
fn alloc_catalog_id(&mut self) -> i64 {
    let id = self.next_catalog_id;
    self.next_catalog_id += 1;
    id
}

// Allocate a data file ID
fn alloc_file_id(&mut self) -> i64 {
    let id = self.next_file_id;
    self.next_file_id += 1;
    id
}
```

The new snapshot INSERT always includes the updated counters:

```rust
// snapshot_id = self.last_snapshot_id + 1
// new_next_catalog_id = self.next_catalog_id (after all allocations for this batch)
// new_next_file_id = self.next_file_id (after all allocations for this batch)
client.execute(
    "INSERT INTO ducklake_snapshot \
     (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) \
     VALUES ($1, now(), $2, $3, $4)",
    &[&new_snapshot_id, &new_schema_version, &self.next_catalog_id, &self.next_file_id],
).await?;
```

SlateDuck validates that supplied IDs don't conflict with its internal counter, returning
`SQLSTATE 23505` on duplicate or `SQLSTATE 40001` on stale reader state.

### 4.5 Bootstrap Without DDL or `ON CONFLICT`

```rust
async fn bootstrap_namespace(&mut self, namespace: &str) -> Result<i64, RelayError> {
    // Check cache first
    if let Some(&id) = self.schema_ids.get(namespace) {
        return Ok(id);
    }
    // Try SELECT
    let existing = self.db.query_opt(
        "SELECT schema_id FROM ducklake_schema WHERE schema_name = $1",
        &[&namespace],
    ).await?;

    let schema_id = match existing {
        Some(r) => r.get(0),
        None => {
            let id = self.alloc_catalog_id();
            let uuid = uuid::Uuid::new_v4().to_string();
            self.db.execute(
                "INSERT INTO ducklake_schema \
                 (schema_id, schema_name, schema_uuid) \
                 VALUES ($1, $2, $3)",
                &[&id, &namespace, &uuid],
            ).await?;
            id
        }
    };
    self.schema_ids.insert(namespace.to_string(), schema_id);
    Ok(schema_id)
}
```

Same pattern for `bootstrap_table()` and `ensure_column()`. All cached after first call — the
SELECT round trips only happen once per relay process lifetime per unique namespace/table.

### 4.6 Offset Tracking via `ducklake_metadata`

Since `tide.ducklake_offset_map` doesn't exist in SlateDuck, relay state is persisted in
`ducklake_metadata` using structured keys:

```
-- Outbox offset checkpoint (written atomically inside the snapshot commit transaction)
INSERT INTO ducklake_metadata (scope, scope_id, metadata_key, value)
VALUES ('global', 0,
        'pg_tide.{pipeline_name}.offset',
        '{outbox_offset}')
-- SlateDuck interprets this as a standard metadata upsert
```

On source/sink startup, the relay reads back its last committed offset:

```sql
SELECT value FROM ducklake_metadata
WHERE metadata_key = 'pg_tide.{pipeline_name}.offset'
```

This gives soft exactly-once: if the relay crashes after committing to SlateDuck but before
acknowledging the outbox, the next run will re-read events starting from the checkpoint. The
relay's inbox idempotency key (`_dedup_key`) prevents double-processing.

### 4.7 Inlined Data Tables — SlateDuck Native Behaviour

In the PostgreSQL path, the sink issues `CREATE TABLE ducklake_inlined_data_{T}_{V}` itself.
In SlateDuck, inlined data tables are handled internally via the `0xFD` dynamic prefix; the
sidecar creates the in-memory table layout automatically when it first receives an `INSERT INTO
ducklake_inlined_data_{T}_{V}` from the client.

The SlateDuck sink should therefore:
1. **Not** issue `CREATE TABLE ducklake_inlined_data_*` — just INSERT directly.
2. Use the standard DuckLake inlined INSERT pattern within a `BEGIN` / `COMMIT` block.
3. Track `inlined_tables_ready` by whether the INSERT succeeded, not by whether a CREATE ran.

### 4.8 Schema Evolution in SlateDuck

Schema evolution in DuckLake v1.0 is just `INSERT INTO ducklake_column` — no `ALTER TABLE`.
SlateDuck supports `INSERT INTO ducklake_column` fully. The `add_column_additive()` method
needs only to remove the `ON CONFLICT DO UPDATE` and `RETURNING` clauses:

```rust
// Check if column exists
let existing = db.query_opt(
    "SELECT column_id FROM ducklake_column \
     WHERE table_id = $1 AND column_name = $2 AND end_snapshot IS NULL",
    &[&table_id, &col_name],
).await?;

if existing.is_none() {
    let col_id = self.alloc_catalog_id();
    db.execute(
        "INSERT INTO ducklake_column \
         (column_id, table_id, column_name, column_type, column_order, nullable, begin_snapshot) \
         VALUES ($1, $2, $3, 'VARCHAR', $4, true, $5)",
        &[&col_id, &table_id, &col_name, &col_order, &new_snapshot_id],
    ).await?;
    self.column_ids.insert((table_id, col_name.to_string()), col_id);
    self.schema_changed = true;
}
```

Setting `self.schema_changed = true` causes the next snapshot's `schema_version` to increment,
which invalidates DuckDB's schema cache correctly — exactly as SlateDuck's design doc requires.

---

## 5. Implementation Phases

### Phase 0 — Prerequisite: Upgrade to Real DuckLake v1.0 Spec (Shared with ducklake.md)

**Owner:** Team (shared with DuckLake PostgreSQL upgrade)  
**Effort:** 2–3 weeks  
**Blocks:** All SlateDuck phases

The current `DuckLakeSink` writes a custom catalog schema that differs from the DuckLake v1.0
spec in multiple ways (no `next_catalog_id` / `next_file_id`, per-table snapshots, 8 tables
instead of 28). Both the PostgreSQL and SlateDuck paths need the v1.0 spec.

**Deliverables:**
- Extract `ducklake_common::{parquet, column_stats, catalog_ids, SchemaChangePolicy, DuckLakePartition}` into a shared module
- Rewrite `DuckLakeSink::ensure_catalog()` to create the real 28-table DuckLake v1.0 schema (with sequences for the PG path)
- Rewrite `DuckLakeSink::publish()` to write to `ducklake_snapshot` with `next_catalog_id` / `next_file_id`
- Add integration tests connecting DuckDB to the relay-written catalog to verify end-to-end queryability
- All existing relay tests pass

---

### Phase 1 — `SlateDuckSource`: Read Path

**Effort:** 1 week  
**Depends on:** Phase 0

Implement `pg-tide-relay/src/source/slateduck.rs`:

1. **Startup cache population:**
   - `SELECT schema_id FROM ducklake_schema WHERE schema_name = $1` — no JOIN
   - `SELECT table_id FROM ducklake_table WHERE schema_id = $1 AND table_name = $2`
   - `SELECT value FROM ducklake_metadata WHERE metadata_key = 'pg_tide.{pipeline}.offset'` — load last offset
   - Cache all three; only re-query metadata key on each poll

2. **Snapshot poll (per poll cycle):**
   - `SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $1` (catalog-wide)
   - If unchanged: return empty, sleep `snapshot_poll_interval_ms`

3. **Data file fetch:**
   - `SELECT file_id, file_path, record_count, begin_snapshot, file_size_bytes FROM ducklake_data_file WHERE table_id = $1 AND begin_snapshot > $2 AND begin_snapshot <= $3 AND (end_snapshot IS NULL OR end_snapshot > $3) ORDER BY begin_snapshot ASC`
   - Remove `LIMIT` if SlateDuck doesn't support parameterised LIMIT; implement client-side slicing

4. **Acknowledge:**
   - Advance `last_snapshot_id` in memory; no DB write needed at this level (offset is written by `SlateDuckSink` or a separate checkpoint call)

**Tests:**
- Unit: mock PG-wire responses, verify sequential lookups produce correct results
- Integration (when SlateDuck Phase 4 available): connect to a real SlateDuck sidecar

---

### Phase 2 — `SlateDuckSink`: Parquet Write Path

**Effort:** 2 weeks  
**Depends on:** Phase 0

Implement `pg-tide-relay/src/sink/slateduck.rs` — Parquet write path (batches above
`inline_row_limit`):

1. **`verify_catalog_ready()`** — replaces `ensure_catalog()`:
   ```sql
   SELECT value FROM ducklake_metadata WHERE metadata_key = 'version'
   ```
   Returns `RelayError::Config` with a helpful message if the catalog isn't initialized
   (directing the user to run `slateduck init`).

2. **`refresh_id_state()`** — load `next_catalog_id`, `next_file_id`, `last_snapshot_id`
   from the most recent snapshot (see §4.4 above).

3. **`bootstrap_namespace()` / `bootstrap_table()` / `ensure_column()`** — SELECT + conditional
   INSERT, no `ON CONFLICT`, no `RETURNING` (see §4.5 above).

4. **`publish()` — Parquet path (within `BEGIN` / `COMMIT`):**
   - Allocate `snapshot_id = last_snapshot_id + 1`, `file_id = alloc_file_id()`
   - Write Parquet to object storage (reuse `ducklake_common::parquet`)
   - `INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (...)`
   - `INSERT INTO ducklake_data_file (...) VALUES (...)`
   - `INSERT INTO ducklake_snapshot_changes (...) VALUES (...)`
   - Per-column `INSERT INTO ducklake_file_column_stats (...) VALUES (...)` — plain INSERT, no upsert
   - `UPDATE ducklake_table_stats SET record_count = record_count + $1, ... WHERE table_id = $2`
   - Table-level column stats: SELECT first, then INSERT or UPDATE (see §3.10)
   - `COMMIT`
   - Persist offset to `ducklake_metadata` (single follow-up INSERT after commit — not inside the same txn since SlateDuck's `ducklake_metadata` is written separately)

5. **No `pg_notify()`.**

**Tests:**
- Unit: verify INSERT shapes against SlateDuck's spec query corpus
- Property test: `refresh_id_state()` → allocate N IDs → commit → refresh → allocated IDs are
  strictly greater than all previous IDs

---

### Phase 3 — `SlateDuckSink`: Inlined Data Path

**Effort:** 1 week  
**Depends on:** Phase 2

1. For batches `<= inline_row_limit` rows, use DuckLake's inlined data protocol:
   - **No `CREATE TABLE`** — INSERT directly into `ducklake_inlined_data_{table_id}_{schema_version}`
   - SlateDuck creates the in-memory layout on first INSERT (via its `0xFD` prefix handler)
   - Track `inlined_tables_ready` set to avoid repeated first-INSERT detection overhead

2. Inline `INSERT` within `BEGIN` / `COMMIT`:
   ```sql
   INSERT INTO ducklake_inlined_data_{table_id}_{schema_version}
   (row_id, begin_snapshot, _dedup_key, _subject, _op, _outbox_id, data)
   VALUES ($1, $2, $3, $4, $5, $6, $7)
   ```

3. Update `ducklake_table_stats` and `ducklake_snapshot_changes` as in the Parquet path.

4. `ducklake_inlined_data_tables` registry: `INSERT INTO ducklake_inlined_data_tables (table_id, schema_version) VALUES (...)`— confirm whether SlateDuck requires explicit registration or handles it implicitly.

5. Enforce SlateDuck's 64 MiB batch limit (section 5.21 of SlateDuck design doc) and return
   `SQLSTATE 54001` if exceeded.

---

### Phase 4 — Schema Evolution

**Effort:** 3 days  
**Depends on:** Phase 2

Reuse `ducklake_common::schema_evolution::detect_new_json_keys()`. Adapt
`add_column_additive()`:
- No `ON CONFLICT`, no `RETURNING` (see §4.8 above)
- Set `self.schema_changed = true` → new snapshot gets incremented `schema_version`
- Test the full matrix: schema-changing ops increment `schema_version`; data-only ops do not

---

### Phase 5 — Auto-Partition via `ducklake_metadata`

**Effort:** 2 days  
**Depends on:** Phase 2

Replace `tide.ducklake_partition_config` INSERTs with `ducklake_metadata` entries:

```sql
INSERT INTO ducklake_metadata (scope, scope_id, metadata_key, value)
VALUES ('global', 0,
        'pg_tide.partition.{pipeline_name}.{namespace}.{table}',
        '{partition_type}')
```

No `ON CONFLICT` — check existence first with a SELECT and skip if already set. Cache in
`partition_registered` set as before.

---

### Phase 6 — Integration Testing and Validation

**Effort:** 2 weeks  
**Depends on:** SlateDuck Phase 4 (Strategy B PG-wire sidecar) reaching usable state

1. **Wire corpus capture:** Use `tcpdump` or `pgwire` tracing to capture every SQL statement the
   `SlateDuckSink` and `SlateDuckSource` issue against a SlateDuck sidecar. Commit to
   `tests/fixtures/wire-corpus/pgtide-slateduck-{version}.jsonl`. Share with the SlateDuck
   project as a Phase 0 corpus contribution.

2. **End-to-end test:**
   - Stand up a SlateDuck sidecar against LocalFS (using SlateDuck's own dev tooling)
   - Publish 100 events through a pg-tide outbox
   - Relay with `SlateDuckSink`
   - Connect DuckDB to the SlateDuck endpoint
   - `SELECT count(*) FROM events` → 100
   - `SELECT * FROM events AT (SNAPSHOT => 5)` → time travel works
   - Kill relay mid-batch, restart → zero duplicate events (idempotency via `_dedup_key`)

3. **Source test:**
   - Write events to SlateDuck directly via DuckDB
   - `SlateDuckSource` picks them up and delivers to pg-tide inbox
   - Verify deduplication on replay

4. **Compatibility matrix:** Test against each SlateDuck release as they land, mirroring the
   DuckDB compatibility matrix approach in SlateDuck's own design (§5.11).

---

### Phase 7 — Production Hardening

**Effort:** 1 week

1. **Reconnection on fencing (`SQLSTATE 57P04`):** When the SlateDuck writer is fenced (writer
   takeover), the sidecar returns `57P04 admin_shutdown`. The relay must detect this, close the
   connection, and retry with exponential backoff. Map to `RelayError::SinkPublish` with a
   retryable flag.

2. **Read-only replica routing:** When a sidecar returns `25006 read_only_sql_transaction`
   (reader pod received a write), retry against the configured write endpoint (from
   `ducklake_metadata` key `0xFF | "writer-endpoint"` if accessible, or from static config).

3. **Object-store throttle handling (`SQLSTATE 08006`):** Exponential backoff with jitter for
   S3 throttling responses propagated through the SlateDuck sidecar.

4. **Metrics:** Expose `slateduck_snapshot_commit_duration_ms`, `slateduck_catalog_connect_retries`,
   `slateduck_id_allocation_batch_size`, `slateduck_inline_rows_written_total`,
   `slateduck_parquet_files_written_total` via the relay's existing Prometheus registry.

---

## 6. Configuration Reference

```toml
# pg-tide relay TOML — SlateDuck sink
[[pipeline]]
name = "orders-to-lake"

[pipeline.source]
type = "outbox"
connection = "postgres://app:secret@pg:5432/app"
outbox = "orders"

[pipeline.sink]
type = "slateduck"
# PG-wire TCP endpoint of the SlateDuck sidecar (Strategy B)
sidecar_endpoint = "host=slateduck-writer.svc port=5432"
# Object storage root for Parquet data files
data_path = "s3://my-bucket/data/pgtide"
namespace = "pgtide"
table_template = "{stream_table}"
compression = "snappy"           # snappy | zstd | none
inline_row_limit = 10            # batches <= this go to inlined-data path
on_schema_change = "warn_and_continue"  # pause | route_to_dlq | warn_and_continue | auto_new_stream
partition = "daily"              # none | daily | monthly | bucket:N
pipeline_name = "orders-to-lake" # used for offset tracking in ducklake_metadata
# NOTE: atomic_lake_writes is NOT available for SlateDuck.
# SlateDuck and the pg-tide outbox are separate systems.
# Exactly-once is provided via _dedup_key idempotency.
```

```toml
# pg-tide relay TOML — SlateDuck source (reverse relay: lake → inbox)
[[pipeline]]
name = "enriched-orders-from-lake"

[pipeline.source]
type = "slateduck"
sidecar_endpoint = "host=slateduck-writer.svc port=5432"
namespace = "enriched"
table = "orders"
snapshot_poll_interval_ms = 2000
consumer_group = "pgtide-inbox-consumer"

[pipeline.sink]
type = "inbox"
connection = "postgres://app:secret@pg:5432/app"
inbox = "enriched_orders"
```

---

## 7. Exactly-Once Semantics and Trade-offs

In the PostgreSQL-backed DuckLake path, the relay can achieve true exactly-once delivery by
committing the outbox offset advance and the DuckLake snapshot in a **single PostgreSQL
transaction** (`atomic_lake_writes = true`). This is the relay's strongest correctness
guarantee and a key differentiator.

With SlateDuck, the outbox (PostgreSQL) and the catalog (SlateDB in S3) are **different storage
systems**. A single cross-system transaction is not possible.

The relay instead provides **at-least-once with idempotent deduplication**:

```
State machine per relay run:
1. Fetch batch of N events from outbox (PostgreSQL)
2. Write Parquet to S3 (data plane — SlateDuck doesn't control this)
3. Commit catalog snapshot to SlateDuck (catalog plane)
4. Persist offset to ducklake_metadata (inside the same SlateDuck transaction as step 3)
5. Acknowledge events in outbox (PostgreSQL)

Crash scenarios:
- Crash after step 1: no side effects; replay from same offset
- Crash after step 2: orphaned Parquet file on S3; SlateDuck GC cleans it up
- Crash after step 3+4: SlateDuck has the snapshot + offset; on restart, relay
  reads offset from ducklake_metadata and skips events already committed
- Crash after step 3 but before step 4: relay re-runs same batch;
  _dedup_key prevents duplicate rows in the inlined-data path;
  Parquet path may get duplicate files (SlateDuck GC will clean orphans)
```

The `_dedup_key` field (always present in relay messages) provides the idempotency for inlined
data. For the Parquet path, a duplicate file created by a retry results in duplicate rows visible
to DuckDB — this is the weakest point. Mitigations:

1. Make the Parquet filename deterministic: `snap_{snapshot_id}_{batch_hash}.parquet` so a retry
   writes the same path (object_store `put` is idempotent for the same key).
2. Use SlateDuck's `end_snapshot` mechanism: if the retry detects the prior snapshot was committed
   (by reading `ducklake_metadata` offset), skip the Parquet write entirely.

Document this clearly in the user-facing docs: "For hard exactly-once guarantees to a DuckLake
catalog, use the PostgreSQL-backed DuckLake sink with `atomic_lake_writes = true`."

---

## 8. Dependency on SlateDuck Roadmap

SlateDuck's Phase 4 (Strategy B PG-wire sidecar) is the prerequisite for any relay integration
testing. As of May 2026, SlateDuck is in Phase 0 (project bootstrap). The pg-tide relay work
can proceed in parallel:

| SlateDuck Phase | pg-tide relay work that can proceed |
|---|---|
| Phase 0 (now) | Phases 0–5 of this plan (code complete, no live sidecar needed) |
| Phase 0 validation artifacts shipped | Share relay SQL corpus as a Phase 0 contribution to SlateDuck |
| Phase 4 alpha (PG-wire sidecar) | Phase 6 integration testing begins |
| Phase 4 stable | Phase 7 hardening; announce joint integration |
| Phase 6 (SlateDuck operational hardening) | Leverage `slateduck inspect` / `slateduck verify` in relay diagnostics |

**Coordination opportunity:** The relay's `SlateDuckSink` SQL statements constitute a new DuckLake
client corpus. Contributing `tests/fixtures/wire-corpus/pgtide-slateduck-{version}.jsonl` to the
SlateDuck project serves their Phase 0 requirement for "every distinct SQL statement the
ducklake extension emits" — and ensures the relay's queries are explicitly validated against
SlateDuck's bounded set before Phase 4 dispatch code is written.

### 8.1 Wire Corpus Contribution Format

The wire corpus JSONL file contributed to SlateDuck must contain one entry per distinct SQL
statement shape. Each entry:

```jsonl
{"id":"sink.refresh_id_state","sql":"SELECT snapshot_id, next_catalog_id, next_file_id, schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1","params":[],"result_columns":["snapshot_id:int8","next_catalog_id:int8","next_file_id:int8","schema_version:int4"],"direction":"write_session","notes":"Called once on startup and after each commit"}
{"id":"sink.insert_snapshot","sql":"INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES ($1, now(), $2, $3, $4)","params":["int8","int4","int8","int8"],"result_columns":[],"direction":"write_session","notes":"One per publish batch"}
{"id":"source.poll_new_snapshot","sql":"SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $1","params":["int8"],"result_columns":["max:int8"],"direction":"read_session","notes":"Called every poll interval"}
```

Fields:
- `id`: Unique identifier within the pg-tide corpus (dot-separated component.operation)
- `sql`: Exact SQL shape with `$N` parameter placeholders
- `params`: Array of PostgreSQL type names for each parameter
- `result_columns`: Array of `name:type` for expected result set columns
- `direction`: `write_session` (connects to writer) or `read_session` (can use reader)
- `notes`: Human-readable context for the SlateDuck team

The corpus will be generated automatically by the Phase 6 wire-capture tooling and committed to
both repos.

---

## 8.2 Multi-Client Considerations

The `SlateDuckSink` and `SlateDuckSource` are designed so that:

1. **DuckDB can read everything the relay writes.** The relay writes spec-compliant DuckLake
   catalog rows. Any DuckDB process connecting to the same SlateDuck sidecar can query the
   relay-written data via standard `ATTACH 'ducklake:postgres:...'` with no awareness of pg-tide.

2. **Other DuckLake writers can coexist** (subject to single-writer constraint). The relay reads
   `next_catalog_id` / `next_file_id` from the latest snapshot — it does not assume it is the
   only writer. If another client (DuckDB, DataFusion) commits a snapshot between relay polls,
   the relay will see the updated counters on its next `refresh_id_state()` call.

3. **Application metadata is namespaced.** All relay state in `ducklake_metadata` uses the
   `pg_tide.` prefix (per SlateDuck §5.19a). Other applications use their own prefixes.
   No collision is possible.

4. **No pg-tide-specific catalog tables.** The relay does NOT create any `tide.*` tables in
   SlateDuck. All state lives in standard DuckLake v1.0 tables + namespaced metadata keys.

5. **Schema evolution is additive and spec-compliant.** New columns are added via
   `INSERT INTO ducklake_column` with incremented `schema_version` in the next snapshot —
   exactly as DuckDB's own DDL path does. A DuckDB user issuing `ALTER TABLE ADD COLUMN`
   on a relay-written table will see correct, merged results.

6. **Parquet file layout follows DuckLake conventions.** File paths use the standard
   `{schema_path}/{table_path}/{file_name}.parquet` hierarchy so DuckDB's file pruning and
   stats-based filtering work optimally.

**Future client implications:** If a future integration (e.g., DataFusion-DuckLake →
SlateDuck) also needs offset tracking, it would use `datafusion_pipeline.{name}.offset` in
`ducklake_metadata` — same mechanism, different namespace. The relay does not need to be
aware of other pipelines writing to the same catalog.

---

## 9. Open Questions

| # | Question | Impact | How to resolve | Current Status |
|---|---|---|---|---|
| Q1 | Does SlateDuck support `LIMIT $N` with parameterised LIMIT in its bounded set? | Source poll batch size | Validate in Phase 0 wire corpus against SlateDuck; fall back to client-side slicing | **Proposed** — filed as an open question in SlateDuck §12. The dispatcher addition is trivial; pg-tide's wire corpus contribution will formalize the request. Interim: use `ORDER BY ... DESC LIMIT 1` (literal) for latest-snapshot reads; implement client-side slicing for data-file batch reads. |
| Q2 | Does SlateDuck require `ducklake_inlined_data_tables` to have an explicit INSERT before the first inlined INSERT, or does it create the entry implicitly? | Phase 3 inlining protocol | Read SlateDuck Phase 4 source when available | **Likely implicit** — SlateDuck's `0xFD` key prefix handler creates the in-memory layout on first INSERT (§5.2 of SlateDuck design doc). However, the relay should INSERT into `ducklake_inlined_data_tables` anyway for DuckDB compatibility (DuckDB reads this registry when listing inlined tables). |
| Q3 | Does SlateDuck's `ducklake_metadata` use `(scope TEXT, scope_id BIGINT, metadata_key TEXT, value TEXT)` or a different schema? | Offset tracking key format | Read DuckLake v1.0 spec source table; confirm with SlateDuck | **Confirmed** — SlateDuck §5.19a defines scoped key/value with `(scope, scope_id, metadata_key, value)`. Application keys use `pg_tide.{pipeline_name}.{key}` convention with `scope = 'global'`, `scope_id = 0`. |
| Q4 | What isolation level does SlateDuck's `BEGIN` use by default? Does it accept `BEGIN ISOLATION LEVEL SERIALIZABLE`? | Phase 2 transaction model | Phase 0 wire capture | **Snapshot isolation** — SlateDB provides `SerializableSnapshot` as its strongest level. SlateDuck's `BEGIN` defaults to this. Whether `BEGIN ISOLATION LEVEL SERIALIZABLE` is accepted as a no-op is filed as an open question in SlateDuck §12. Use plain `BEGIN` in the relay implementation. |
| Q5 | Can the relay retrieve `0xFF | "writer-endpoint"` via a SQL query or only via the raw KV API? | Phase 7 writer routing | SlateDuck Phase 4 design | **Unresolved** — depends on whether SlateDuck exposes internal KV keys through its PG-wire interface. Likely not: use static endpoint config for v1 and revisit if SlateDuck adds a metadata introspection endpoint. |
| Q6 | Does SlateDuck surface SlateDB's `GcError` as a distinct SQLSTATE, or only as `XX000`? | Phase 7 error handling | SlateDuck Phase 4 implementation | **Unresolved** — SlateDuck's error mapping will be defined in Phase 4. The relay should handle both distinct SQLSTATEs and `XX000` with descriptive error messages. |
| Q7 | Is `gen_random_uuid()` available in SlateDuck's PG-wire shim? | Bootstrap table INSERT | Check SlateDuck handshake corpus; fall back to Rust-side UUID generation | **Decision: use Rust-side UUIDs** — SlateDuck §4.3 client onboarding section confirms the preferred approach is for clients to generate UUIDs and supply them as literal string parameters. This avoids requiring SlateDuck to implement PG functions. The relay will use `uuid::Uuid::new_v4()` and pass as `$1`. |

---

## 10. Success Criteria

1. `SlateDuckSink::publish()` issues **zero** SQL statements outside SlateDuck's bounded set
   (validated by wire corpus replay tests).
2. `SlateDuckSource::poll()` issues **zero** multi-table JOINs or subqueries.
3. End-to-end test: 10,000 events published from a pg-tide outbox, relayed through
   `SlateDuckSink`, queryable via DuckDB `ATTACH 'ducklake:postgres:...'` with zero discrepancies.
4. Time-travel test: `SELECT count(*) FROM events AT (SNAPSHOT => N)` returns the correct
   count for snapshots N=1..max.
5. Crash recovery test: relay killed at each of the 5 crash points in §7; on restart, final
   event count is exactly 10,000 (no duplicates in the inlined-data path; at most 1 duplicate
   file in the Parquet path, cleaned by SlateDuck GC).
6. Fencing test: SlateDuck sidecar writer taken over; relay detects `57P04`, reconnects, and
   resumes within 30 seconds.
7. `SQLSTATE 54001` returned when a batch exceeds the 64 MiB SlateDuck limit; relay routes
   batch to DLQ.
8. Zero `nextval()`, `CREATE TABLE`, `ON CONFLICT`, `pg_notify`, or `tide.*` references in
   `slateduck.rs` (enforced by a `grep` CI check).

---

## 11. Why This Is Worth Building

The combination of pg-tide + SlateDuck creates something neither project can offer alone:

- **For serverless / spot deployments:** Events leave PostgreSQL atomically via the outbox,
  land in S3 as a queryable lakehouse, with no Kafka cluster, no Flink jobs, no persistent
  database for the lake. The only always-on infrastructure is the application's PostgreSQL
  instance and a stateless SlateDuck sidecar (which can itself run as a spot pod with
  automatic writer election).

- **For cost-sensitive workloads:** S3 Standard at $0.023/GB-month vs. RDS at $0.115/GB-month
  (instance) + $0.115/GB-month (storage) makes a 5× cost difference for the catalog. For
  companies ingesting millions of events per day, this is meaningful.

- **For the ecosystem:** pg-tide becomes the first event streaming system with production-grade
  SlateDuck support, reaching SlateDuck's early adopters and giving them a battle-tested
  PostgreSQL-outbox → lake pipeline on day one.

- **For the DuckLake community:** SlateDuck needs a corpus of real client SQL to validate its
  Phase 4 PG-wire dispatcher. The pg-tide relay's `SlateDuckSink` queries are a second client
  corpus alongside DuckDB's own — helping SlateDuck find edge cases before GA.
