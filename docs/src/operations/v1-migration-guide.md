# v1.0.0 Migration Guide

> **Audience:** Operators and application developers upgrading from any v0.x release to v1.0.0 GA.

## Overview

pg_tide v1.0.0 is the first General Availability release.
It includes several **breaking changes** from the v0.x series that require explicit action
before upgrading.  This guide covers each breaking change, the upgrade procedure,
and the rollback procedure.

---

## Breaking Changes

### 1. Positional SQL API variants removed

The following function signatures have been **removed** in v1.0.0:

| Deprecated form (removed) | Replacement |
|---|---|
| `tide.relay_set_outbox(name, outbox, sink, config, batch_size, enabled)` | `tide.relay_set_outbox_v2(config JSONB)` |
| `tide.relay_set_inbox(name, inbox, config, batch_size, source, enabled, max_retries, idempotent)` | `tide.relay_set_inbox_v2(config JSONB)` |

These forms were deprecated since v0.18.0 and have emitted a `WARNING` since v0.30.0.

**Action required:** Search your application code and migration files for calls to the
positional forms and replace them with the JSONB form before upgrading.

**Example migration:**

```sql
-- Before (deprecated positional form):
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

### 2. KMS envelope format (planned)

v1.0.0 introduces optional envelope encryption with KMS-backed key management.
The envelope format for encrypted messages includes a versioned header that is not
backward-compatible with unencrypted payloads processed by v0.x decoders.

**Action required:** If you use custom downstream consumers that parse the native
pg_tide wire format, review the `docs/src/wire-formats/native.md` KMS section before
upgrading.

---

## Upgrade Procedure

### Step 1: Update your application code

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
