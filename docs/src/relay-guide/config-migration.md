# Migrating from TOML Configuration to Catalog-First Config

This guide explains how to migrate an existing `pg-tide` relay from TOML file–based
configuration to the catalog-first (`catalog_only`) mode introduced in v0.28.0.

---

## Background

Prior to v0.28.0, all relay pipeline configuration was defined in a TOML file and loaded
at startup. The relay would read the file, construct pipeline workers, and start polling.

Starting with v0.28.0, the relay also supports **catalog-first** configuration, where
pipeline definitions are stored entirely in the PostgreSQL catalog (`tide` schema tables)
and loaded at runtime. This enables dynamic reconfiguration without relay restarts.

The `--config-mode` flag (or `PG_TIDE_CONFIG_MODE` env var) controls the mode:

| Value | Description |
|---|---|
| `toml_allowed` (default) | TOML file is read; catalog entries supplement or override it |
| `catalog_only` | TOML file is ignored; all config must exist in the catalog |

---

## Step 1: Export your TOML config to the catalog

Run the migration helper command:

```bash
pg-tide migrate-config --postgres-url "postgres://user:password@host:5432/mydb"
```

This command prints the SQL `INSERT` statements needed to reproduce your TOML pipeline
definitions in the `tide` catalog tables. Review the output before applying it.

To apply directly (review first!):

```bash
pg-tide migrate-config --postgres-url "$DATABASE_URL" | psql "$DATABASE_URL"
```

---

## Step 2: Verify the catalog entries

Connect to the database and confirm the pipelines were inserted:

```sql
SELECT pipeline_name, sink_type, source_type
FROM tide.tide_relay_catalog;
```

Expected output: one row per pipeline that was defined in your TOML file.

---

## Step 3: Run the doctor

```bash
pg-tide doctor --postgres-url "$DATABASE_URL"
```

All checks should pass, including:
- `tide.relay_delivery_receipts` INSERT privilege (v0.28.0)
- `lo_get` / `lo_unlink` EXECUTE privilege (for claim-check support)

---

## Step 4: Switch to catalog-only mode

Update your relay startup command or container environment:

```bash
# CLI flag
pg-tide relay --config-mode catalog_only --postgres-url "$DATABASE_URL"

# Or via environment variable
export PG_TIDE_CONFIG_MODE=catalog_only
pg-tide relay --postgres-url "$DATABASE_URL"
```

In a Kubernetes deployment using the Helm chart:

```yaml
# values.yaml
relay:
  env:
    - name: PG_TIDE_CONFIG_MODE
      value: catalog_only
```

---

## Step 5: Validate

Watch the relay logs for a few minutes:

- Pipeline workers should start without "failed to read TOML" warnings.
- Messages should flow normally through all pipelines.
- Delivery receipts should appear in `tide.relay_delivery_receipts`.

---

## Rollback

To revert to TOML mode, remove the `--config-mode catalog_only` flag (or unset
`PG_TIDE_CONFIG_MODE`). The relay will fall back to `toml_allowed` and read the TOML
file again on the next restart.

Catalog entries are preserved and do not need to be removed unless you want a clean state.

---

## FAQ

**Q: Do I need to remove my TOML file when using `catalog_only`?**  
No. The file is simply ignored. You may delete it once you have verified the migration.

**Q: What happens if a catalog entry conflicts with a TOML entry in `toml_allowed` mode?**  
In `toml_allowed` mode, the TOML file is authoritative. Catalog entries are merged as
supplements. If both define the same pipeline name, the TOML definition wins.

**Q: Can I add new pipelines to the catalog while the relay is running?**  
Yes. The relay reloads catalog entries on each polling cycle. New pipelines become active
within one polling interval without a restart.
