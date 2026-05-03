# pg_tide — Development Guidelines

## Project Overview

PostgreSQL 18 extension (`pg_tide`) providing transactional outbox, idempotent inbox,
and relay catalog. Extracted from pg_trickle v0.46.0 as a standalone module.

**Schema:** `tide`  
**Companion binary:** `pg-tide` (the relay binary in `pg-tide-relay/`)

Key docs: [README.md](README.md) · [sql/pg_tide--0.1.0.sql](sql/pg_tide--0.1.0.sql)

---

## Workflow — Always Do This

After **any** code change:

```bash
just fmt          # Format code
just lint         # clippy + fmt-check (must pass with zero warnings)
```

After changes to SQL-facing code:

```bash
just test-unit         # Pure Rust unit tests (no DB)
```

---

## Coding Conventions

### Error Handling

- Define errors in `pg-tide-ext/src/error.rs` as `PgTideError` enum variants.
- Never `unwrap()` or `panic!()` in code reachable from SQL.
- Propagate via `Result<T, PgTideError>`; convert at the API boundary with `pgrx::error!()`.

### SPI

- All catalog access via `Spi::connect()` or `Spi::get_one_with_args()`.
- Keep SPI blocks short — no long operations while holding a connection.
- **Always cast `name`-typed columns to `text`** when fetching into Rust `String`.

### Unsafe Code

- Minimize `unsafe` blocks. Every `unsafe` block must have a `// SAFETY:` comment.

### Logging

- Use `pgrx::log!()`, `info!()`, `warning!()`, `error!()`.
- Never `println!()` or `eprintln!()`.

### SQL Functions

- Annotate with `#[pg_extern(schema = "tide")]`.
- All catalog tables live in schema `tide`.

---

## Module Layout

```
pg-tide-ext/src/
├── lib.rs       # Extension entry point, pg_module_magic!()
├── error.rs     # PgTideError enum
├── outbox.rs    # Outbox create/publish/drop/status/consumer-groups
├── inbox.rs     # Inbox create/mark-processed/mark-failed/replay
└── relay.rs     # Relay pipeline config management

pg-tide-relay/src/
├── main.rs         # Binary entry point (pg-tide)
├── cli.rs          # CLI argument parsing
├── config.rs       # TOML config structures
├── coordinator.rs  # Pipeline orchestration
├── envelope.rs     # Message envelope format
├── error.rs        # RelayError enum
├── metrics.rs      # Prometheus metrics
├── transforms.rs   # Subject template rendering
├── sink/           # Message sink implementations
└── source/         # Message source implementations
```

---

## Code Review Checklist

- [ ] No `unwrap()` / `panic!()` in non-test code
- [ ] All `unsafe` blocks have `// SAFETY:` comments
- [ ] SPI connections are short-lived
- [ ] New SQL functions use `#[pg_extern(schema = "tide")]`
- [ ] Error messages include context (outbox/inbox name)
