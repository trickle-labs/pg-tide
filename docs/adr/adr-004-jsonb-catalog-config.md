# ADR-004: JSONB Catalog Config

**Status:** Accepted  
**Date:** 2026-05-05  
**Author:** pg_tide Contributors  

## Context

The relay catalog (`tide.relay_outbox_config`, `tide.relay_inbox_config`)
must store pipeline configuration in a way that:

1. Is queryable and patchable from SQL without application changes.
2. Can evolve across versions without requiring column-level migrations.
3. Is schema-validated at relay startup time.
4. Supports per-backend fields that differ widely (NATS has `subject`, Kafka
   has `topic`, `brokers`, `group_id`, etc.).

**Candidates evaluated:**

- **JSONB column** — a single `config JSONB` column stores the entire pipeline
  config as a typed document.
- **Normalised relational tables** — separate columns for every supported
  backend field; nullable for unused backends.
- **EAV (entity-attribute-value)** — a key-value config table.

## Decision

We store pipeline configuration as a **single `config JSONB` column** in
`tide.relay_outbox_config` and `tide.relay_inbox_config`.

The config document follows a canonical shape:

```json
{
  "source_type": "outbox",
  "source": { "outbox": "orders" },
  "sink_type": "nats",
  "sink": { "url": "nats://localhost:4222", "subject": "orders.events" },
  "batch_size": 100
}
```

Rationale:

1. **Schema-free extensibility** — Adding a new backend field requires no DDL.
   The `validate-config` CLI validates the shape at runtime against the relay's
   known config struct.
2. **Single source of truth** — The entire pipeline description lives in one
   column; operators can `UPDATE tide.relay_outbox_config SET config = …` to
   reconfigure a pipeline.
3. **JSON Schema validation** — A versioned JSON Schema (stored in the relay
   binary) validates configs at coordinator startup and in `validate-config`.
4. **Operational simplicity** — `SELECT config FROM tide.relay_outbox_config`
   is the only query needed to understand any pipeline.

## Consequences

- **Positive**: No DDL required for new backends or config keys.
- **Positive**: `relay_list_configs()` returns a full JSON document that is
  self-describing for operators and tooling.
- **Negative**: Type safety is deferred from SQL DDL to Rust deserialization;
  a typo in a config key produces a runtime error at coordinator startup, not
  a SQL constraint violation. The `validate-config` command mitigates this.
- **Negative**: PostgreSQL JSONB path operators are required for fine-grained
  queries (e.g. `WHERE config->>'sink_type' = 'kafka'`); SQL tooling that
  expects column types may need adjustment.
