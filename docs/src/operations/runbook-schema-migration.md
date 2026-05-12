# Runbook: Schema Migration

**Applies to:** pg_tide PostgreSQL extension  
**Scope:** How to upgrade the pg_tide extension schema without relay downtime.

---

## Overview

pg_tide uses the standard PostgreSQL extension upgrade mechanism:

```sql
ALTER EXTENSION pg_tide UPDATE;
```

This command applies the appropriate `pg_tide--<from>--<to>.sql` upgrade
script atomically within a transaction.  The relay can continue running
during the upgrade with at most a brief window of elevated latency.

---

## Pre-Migration Checklist

Before upgrading:

1. **Back up the database** (or ensure your point-in-time recovery is current).
2. **Check the current version:**
   ```sql
   SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
   ```
3. **Check the available target version:**
   ```sql
   SELECT * FROM pg_available_extension_versions WHERE name = 'pg_tide';
   ```
4. **Run pg-tide doctor** to confirm the relay is healthy before the upgrade:
   ```bash
   pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"
   ```
5. **Review the CHANGELOG** for any breaking changes or required manual steps
   in the target version.

---

## Upgrade Procedure

### 1. Deploy the New Extension Files

Copy the new `.so` library, control file, and SQL migration files to the
PostgreSQL `$libdir` and share directory.  For package-based installs:

```bash
# Debian/Ubuntu:
apt-get install pg-tide=0.19.0

# CNPG (CloudNativePG) — update the cluster manifest image tag:
kubectl patch cluster my-pg --type=merge \
  -p '{"spec":{"imageName":"ghcr.io/my-org/pg-tide-cnpg:0.19.0"}}'
```

### 2. Apply the Migration

```sql
-- Connect as a superuser or the extension owner:
ALTER EXTENSION pg_tide UPDATE;

-- Verify:
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
```

The relay does **not** need to be stopped.  The upgrade script is
transactional and takes only a brief `AccessShareLock` on affected tables.

### 3. Verify Catalog Integrity

```sql
-- Confirm all expected functions are present:
SELECT routine_name, routine_type
FROM   information_schema.routines
WHERE  routine_schema = 'tide'
ORDER  BY routine_name;

-- Confirm relay config tables are intact:
SELECT COUNT(*) FROM tide.relay_outbox_config;
SELECT COUNT(*) FROM tide.relay_inbox_config;
```

### 4. Run pg-tide doctor Again

```bash
pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"
```

All checks should pass.  If any check fails, see the troubleshooting section
below.

---

## Rolling Back

If the migration must be rolled back:

```sql
-- Extensions cannot be downgraded via ALTER EXTENSION.
-- Restore from backup or use PITR to the pre-upgrade snapshot.
```

PostgreSQL does not support extension downgrade scripts.  Always take a
database snapshot before applying an extension upgrade in production.

---

## Relay Behaviour During Migration

- The relay continues to poll and deliver messages during the upgrade.
- The `ALTER EXTENSION` command takes a brief metadata lock.  In-flight
  batches will complete normally; new polls may be delayed by a few
  milliseconds.
- If the relay encounters a schema error mid-migration (extremely unlikely
  with the standard upgrade path), it will classify it as a permanent error
  and pause the affected pipeline.  Resume with `SELECT tide.relay_enable('...')`.

---

## Multi-Step Upgrade

If you are upgrading across multiple versions (e.g. 0.15.0 → 0.19.0),
PostgreSQL applies each intermediate script automatically:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.19.0';
```

pg_tide ships upgrade scripts for every consecutive version pair, so this
always works without manual intermediate steps.

---

## CNPG (CloudNativePG) Notes

When using CloudNativePG, the extension upgrade happens automatically when
you update the cluster image to a version that includes the new `.so` and
SQL files.  The bootstrap `initdb` / `postInitSQL` section runs
`ALTER EXTENSION pg_tide UPDATE` after the image update.  See
[`examples/cnpg/cluster.yaml`](../../../examples/cnpg/cluster.yaml) for a
reference manifest.

---

## See Also

- [Relay Upgrade runbook](runbook-relay-upgrade.md)
- [Crash Recovery runbook](runbook-crash-recovery.md)
- [CHANGELOG](../../../CHANGELOG.md)
