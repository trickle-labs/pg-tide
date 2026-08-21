# v0.x → v1.0.0 Migration Guide

> **Audience:** Operators and application developers upgrading from any v0.x release
> to v1.0.0 GA.  
> **Last updated:** v0.36.0 (positional API forms removed in v0.36.0)

## Overview

pg_tide v1.0.0 is the first General Availability release.
It includes several **breaking changes** from the v0.x series that require explicit
action before upgrading.  This guide covers each breaking change, the upgrade
procedure, the rollback procedure, and the feature compatibility matrix.

## v0.49.0 focused-surface upgrade

The v0.49.0 extension and relay keep the PostgreSQL outbox path and these
destinations: PostgreSQL inbox, NATS JetStream, Apache Kafka, and HTTPS webhook.
Native JSON and CloudEvents remain the supported envelopes. `stdout` and file
output are diagnostics only.

Before upgrading, inventory the catalog without changing it:

```bash
pg-tide migrate-config --postgres-url "$DATABASE_URL"
pg-tide config export --postgres-url "$DATABASE_URL" > pg-tide-v0.48.0.json
```

The inventory reports reverse pipelines, `pg_trickle` sources, removed sinks,
and removed wire formats with `PGTIDE_CONFIG_UNSUPPORTED_SURFACE` and
`last_version=0.48.0`. Export every affected row, then disable, replace, or
delete it. The `0.48.0 → 0.49.0` SQL migration aborts before dropping any
non-empty unsupported state, so retry the extension update only after the
reported rows and state have been handled.

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.49.0';
```

Use `tide.relay_set_outbox_v2` with `source_type = 'outbox'` (or omit the
source type) and a retained sink. New PostgreSQL destinations should use
`sink_type = 'inbox'`; `pg_outbox` remains a compatibility alias.

If the migration is blocked, leave the database at v0.48.0, keep the export,
and restore the prior relay binary. The migration does not provide a down
migration; a database backup is the rollback boundary.

## v0.50.0 delivery-correctness upgrade

Stop the relay before updating the extension and binary. The adjacent
`0.49.0 → 0.50.0` migration is a no-op compatibility step; it adds no catalog
objects.

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.50.0';
```

Replace the relay binary, then start it with the same configuration. Delivery
remains at least once: a crash after destination acknowledgment and before
checkpoint commit may duplicate an event, but cannot silently lose it. Use
`pg-tide replay execute --pipeline NAME --from-id N --to-id M` for bounded,
checkpoint-neutral replay of retained outbox rows.

---

## Breaking Changes

### 1. Positional SQL API variants removed

The following function signatures were **removed in v0.36.0** and will not exist in v1.0.0:

| Deprecated form (removed) | Replacement |
|---|---|
| `tide.relay_set_outbox(name, outbox, sink, config, batch_size, enabled)` | `tide.relay_set_outbox_v2(config JSONB)` |
| `tide.relay_set_inbox(name, inbox, config, batch_size, source, enabled, max_retries, idempotent)` | `tide.relay_set_inbox_v2(config JSONB)` |

These forms were deprecated since v0.18.0, emitted a `WARNING` on every call since v0.30.0,
and were **removed in v0.36.0** via the `pg_tide--0.35.0--0.36.0.sql` migration script.
After running `ALTER EXTENSION pg_tide UPDATE`, any call to the positional forms will fail
with `ERROR: function tide.relay_set_outbox(...) does not exist`.

**Action required:** Search your application code and migration files for calls to the
positional forms and replace them with the JSONB form before upgrading.

**Example migration:**

```sql
-- Before (deprecated positional form — emits WARNING since v0.30.0):
SELECT tide.relay_set_outbox('my-pipeline', 'my-outbox', 'nats', '{}', 100, true);

-- After (v1.0.0 JSONB form):
SELECT tide.relay_set_outbox_v2('{
  "name": "my-pipeline",
  "outbox": "my-outbox",
  "sink_type": "nats",
  "batch_size": 100,
  "enabled": true
}'::jsonb);
```

To find remaining positional calls in your codebase:

```bash
# Search for the deprecated 6-argument form (look for 6 comma-separated args)
grep -rn 'relay_set_outbox\s*(' your-app/ | grep -v '_v2'
grep -rn 'relay_set_inbox\s*(' your-app/ | grep -v '_v2'
```

### 2. KMS envelope format (encrypted outboxes only)

v1.0.0 introduces optional envelope encryption with KMS-backed key management.
The envelope format for encrypted messages includes a versioned `_enc` header field
that is not backward-compatible with unencrypted payload parsers in v0.x relay
binaries.

**Action required (only if using encrypted outboxes):** If you configure any outbox
with `tide.outbox_encryption_config()`, all relay instances reading that outbox must
be upgraded to v1.0.0 before encryption is activated.  Unencrypted outboxes are
unaffected — no action required.

**Rollback note:** Enabling encryption on an outbox is a one-way operation.  Once
the relay has written `_enc` envelopes, v0.x relay binaries cannot decode them.
Disable encryption and allow the relay to drain the encrypted messages before
downgrading to v0.x.

### 3. New required startup configuration (encrypted outboxes only)

When any pipeline's outbox has `tide.outbox_encryption_config()` set, the relay
binary must have KMS connectivity configured (AWS credentials, GCP service account,
Vault token, or local key file path, depending on `kms_provider`).  The relay will
fail to start if KMS is configured for an outbox but no matching provider credentials
are available at startup.

---

## Upgrade Procedure

### Rolling upgrade order

The safest upgrade path is:

1. **Extension first, relay binary second.**  The v0.33.0 relay binary is compatible
   with the v1.0.0 extension schema.
2. **Multiple relay instances:** upgrade one instance at a time, verifying pipeline
   delivery continues before upgrading the next.

### Step 1: Update your application code

1. Replace all calls to the deprecated positional API forms (see Breaking Change #1).
2. Test in a staging environment with the v0.33.0 binary — any remaining positional
   calls will emit `WARNING` in PostgreSQL logs.
3. Ensure zero positional-form warnings appear in staging before proceeding.

### Step 2: Take a full database backup

```bash
pg_dump -Fc -d "$PG_URL" -f "pg_tide-pre-v1.0.0-$(date +%Y%m%d).dump"
```

### Step 3: Run the extension upgrade

```sql
-- Connect to your PostgreSQL database as superuser or pg_tide extension owner:
ALTER EXTENSION pg_tide UPDATE TO '1.0.0';
```

This applies all incremental migration scripts from your current version to 1.0.0.
The migration is safe to run on a live database — it uses `CREATE TABLE IF NOT EXISTS`,
`CREATE OR REPLACE FUNCTION`, and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, which
do not block ongoing relay operations.

**Verify the upgrade:**

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
-- Expected: 1.0.0
```

### Step 4: Update the relay binary

Replace the `pg-tide` binary with the v1.0.0 release artifact:

```bash
# Example for Linux amd64:
curl -L https://github.com/trickle-labs/pg-tide/releases/download/v1.0.0/pg-tide-linux-amd64 \
     -o /usr/local/bin/pg-tide
chmod +x /usr/local/bin/pg-tide
```

### Step 5: Verify with `--self-test`

```bash
pg-tide --postgres-url "$PG_URL" --self-test --expect-extension-version 1.0.0
```

Exit code 0 = pass.  Exit code 1 = schema incompatibility; see output for details.

### Step 6: Restart the relay

```bash
# Kubernetes:
kubectl rollout restart deployment/pg-tide-relay

# systemd:
systemctl restart pg-tide

# Docker Compose:
docker compose up -d --no-deps pg-tide
```

---

## Rollback Procedure

PostgreSQL extension downgrades are not natively supported.

**Unencrypted deployments (no `outbox_encryption_config` rows):**

1. Stop the v1.0.0 relay binary.
2. Restore from the pre-upgrade backup (Step 2 above).
3. Start the v0.33.0 relay binary.

**Encrypted deployments:**

1. Stop the v1.0.0 relay binary.
2. Disable encryption for all affected outboxes: `DELETE FROM tide.outbox_encryption_config;`
3. Allow the relay to process all remaining `_enc` envelopes (they will be logged as
   errors by a v0.33.0 binary and routed to DLQ).
4. Only after the DLQ is clear, restore from the pre-upgrade backup.

> **Recommendation:** Always take a full database backup before running
> `ALTER EXTENSION pg_tide UPDATE`.

---

## Feature Compatibility Matrix

| Feature | v0.x | v1.0.0 | Notes |
|---|---|---|---|
| Transactional outbox | ✅ | ✅ | Unchanged |
| Idempotent inbox | ✅ | ✅ | Unchanged |
| Forward relay (outbox → external sink) | ✅ | ✅ | Unchanged |
| Reverse relay (external source → inbox) | ✅ | ✅ | Unchanged |
| Connector surface | See generated matrix | See generated matrix | Evidence-based |
| Wire formats (Debezium, Maxwell, Canal, CloudEvents, native) | ✅ | ✅ | Unchanged |
| DuckLake integration | ✅ | ✅ | Unchanged |
| Outbox table partitioning | ✅ | ✅ | Unchanged |
| Multi-tenant relay groups | ✅ | ✅ | Unchanged |
| Pipeline dependency DAG | ✅ | ✅ | Unchanged |
| KMS envelope encryption | ❌ | ✅ | New in v1.0.0 |
| `relay_set_outbox()` 6-param positional form | ✅ (⚠️) | ❌ | Removed |
| `relay_set_inbox()` 8-param positional form | ✅ (⚠️) | ❌ | Removed |
| WAL logical-replication source | 🧪 (`wal-source`) | ❌ | Deferred to v1.1.0 |

⚠️ = deprecated; emits WARNING on every call since v0.30.0.  
🧪 = feature-gated proof-of-concept; not production-ready.

---

## Deprecation Schedule

| Symbol | Deprecated since | Warning activated | Removed in |
|---|---|---|---|
| `tide.relay_set_outbox()` (6-parameter positional form) | v0.18.0 | v0.30.0 | v1.0.0 |
| `tide.relay_set_inbox()` (8-parameter positional form) | v0.18.0 | v0.30.0 | v1.0.0 |

---

## What is NOT in v1.0.0

See [v1-scope.md](../v1-scope.md) for a complete list of features explicitly deferred
to post-v1.0.0 releases to prevent scope creep.  Key exclusions:

- WAL logical-replication source (v1.1.0)
- Kafka exactly-once via transactions (v1.1.0)
- WASM transform plugin system (v1.2.0)
- Web UI control plane (v1.3.0)
- Additional connector ecosystems beyond the current generated matrix (v1.1+)


1. Replace all calls to the deprecated positional API forms (see above).
2. Test in a staging environment with the v0.30.0 binary (which emits `WARNING` for
   any remaining positional calls).

### Step 2: Run the extension upgrade

```sql
-- Connect to your PostgreSQL database and run:
ALTER EXTENSION pg_tide UPDATE TO '1.0.0';
```

This applies all incremental migration scripts from your current version to 1.0.0.

### Step 3: Update the relay binary

Replace the `pg-tide` relay binary with the v1.0.0 release artifact.
The binary and extension must be upgraded in lockstep — do not run a v0.x binary
against a v1.0.0 schema.

### Step 4: Verify with `--self-test`

```bash
pg-tide --postgres-url "$PG_URL" --self-test
```

The self-test will exit 0 on success or report any schema incompatibilities.

---

## Rollback Procedure

PostgreSQL extension downgrades are not natively supported.
To roll back to a v0.x version:

1. Restore from a pre-upgrade database backup.
2. Roll back the relay binary to the previous v0.x release.
3. Do **not** run `ALTER EXTENSION pg_tide UPDATE` until ready to re-upgrade.

> **Recommendation:** Always take a full database backup before running
> `ALTER EXTENSION pg_tide UPDATE`.

---

## Deprecation Schedule

| Symbol | Deprecated since | Removed in |
|---|---|---|
| `tide.relay_set_outbox()` (6-parameter positional form) | v0.18.0 | v1.0.0 |
| `tide.relay_set_inbox()` (8-parameter positional form) | v0.18.0 | v1.0.0 |

---

## What is NOT in v1.0.0

See [v1-scope.md](../v1-scope.md) for a complete list of features explicitly deferred
to post-v1.0.0 releases to prevent scope creep.
