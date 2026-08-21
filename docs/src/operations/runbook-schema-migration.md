# Runbook: Schema Migration

**Applies to:** pg_tide PostgreSQL extension  
**Scope:** How to upgrade the pg_tide extension schema through v0.51.0.

---

## Overview

pg_tide uses the standard PostgreSQL extension upgrade mechanism:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.51.0';
```

This command applies the appropriate `pg_tide--<from>--<to>.sql` upgrade
script atomically within a transaction. Stop relays older than v0.50.0 before
a floor-to-target upgrade; only the v0.50.0/v0.51.0 rolling window is supported.

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
apt-get install pg-tide=0.51.0

# CNPG (CloudNativePG) — update the cluster manifest image tag:
kubectl patch cluster my-pg --type=merge \
  -p '{"spec":{"imageName":"ghcr.io/my-org/pg-tide-cnpg:0.51.0"}}'
```

### 2. Apply the Migration

```sql
-- Connect as a superuser or the extension owner:
ALTER EXTENSION pg_tide UPDATE;

-- Verify:
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
```

Stop pre-v0.50.0 relays before the update. The upgrade is transactional, but
the v0.48.0 → v0.49.0 migration can remove unsupported objects after its
fail-closed preflight, so do not treat every step as a brief metadata change.

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
-- The v0.50.0 -> v0.51.0 boundary is restore/PITR-only after commit.
-- Restore from backup or use PITR to the pre-upgrade snapshot.
```

Only migrations explicitly marked reversible in the v0.51.0 lifecycle policy
have a supported reverse script. Always take a full backup before upgrading;
the v0.50.0 → v0.51.0 boundary uses restore or PITR after commit.

---

## Relay Behaviour During Migration

- Stop pre-v0.50.0 relays before the floor-to-target update. Only the bounded
  v0.50.0/v0.51.0 relay window is supported.
- The `ALTER EXTENSION` command is transactional, but lock duration and
  affected objects depend on the adjacent step. Pause publishing and keep the
  relay stopped whenever the migration procedure requires it.
- If the relay encounters a schema error mid-migration (extremely unlikely
  with the standard upgrade path), it will classify it as a permanent error
  and pause the affected pipeline.  Resume with `SELECT tide.relay_enable('...')`.

---

## Multi-Step Upgrade

For the supported v0.47.0 floor, PostgreSQL applies each packaged adjacent
script automatically:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.19.0';
```

Do not infer support for releases older than v0.47.0 from the presence of
historical SQL files; use the documented restore-and-upgrade procedure.

---

## CNPG (CloudNativePG) Notes

When using CloudNativePG, updating the cluster image only makes the target
extension artifact available. Run an explicit migration Job on the primary
after the target files are available; bootstrap hooks do not update an existing
cluster. See
[`examples/cnpg/cluster.yaml`](../../../examples/cnpg/cluster.yaml) for a
reference manifest.

---

## See Also

- [Relay Upgrade runbook](runbook-relay-upgrade.md)
- [Crash Recovery runbook](runbook-crash-recovery.md)
- [CHANGELOG](../../../CHANGELOG.md)
