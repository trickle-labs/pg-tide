# Contributing to pg_tide

Thank you for contributing to pg_tide!

## Development Setup

See [README.md](README.md) for the full setup guide.

## Workflow

After **any** code change:

```bash
just fmt          # Format code
just lint         # clippy + fmt-check (must pass with zero warnings)
just lint-expect  # No bare expect() in production relay code
```

After changes to SQL-facing code:

```bash
just test-unit    # Pure Rust unit tests (no DB)
```

---

## Code Conventions

### Error Handling

- Define errors in `pg-tide-ext/src/error.rs` (`PgTideError`) and `pg-tide-relay/src/error.rs` (`RelayError`).
- Never `unwrap()` or `panic!()` in code reachable from SQL or from the relay worker loop.
- Propagate via `Result<T, E>`; convert at the API boundary.

### `// SAFETY:` Convention for Infallible `expect()` / `unwrap()`

The project convention is to **not use `.expect()` or `.unwrap()` in production code paths**.
Where an operation is provably infallible and replacing it with `?` propagation would be
misleading or verbose, you may retain the `expect()` — but **you must add a `// SAFETY:`
comment on the immediately preceding line** explaining why the operation cannot fail.

**Format:**

```rust
// SAFETY: <reason the operation is infallible, citing spec or invariant>
let value = something.expect("<short label>");
```

**Example:**

```rust
// SAFETY: Hmac::new_from_slice accepts keys of any non-zero length per HMAC-SHA256 spec
// (RFC 2104 §3); key.as_bytes() is always non-empty for a non-empty config value.
let mut mac = <Hmac<Sha256>>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key size");
```

**What counts as infallible?**

- HMAC/hash operations on byte slices of known-valid length.
- `NonZeroU32::new(n)` where `n` is a compile-time constant `> 0`.
- String operations on inputs that have already been validated (e.g., `parse()` after `validate_identifier()`).

**What does NOT qualify:**

- Network operations (`connect`, `query`, `send`).
- File I/O.
- Lock acquisition.
- Any operation that depends on runtime state.

### CI enforcement

`just lint-expect` and the `lint-expect` CI job scan `pg-tide-relay/src/` for bare `.expect()`
calls that are not preceded by a `// SAFETY:` comment.  Any new bare `expect()` will fail CI.

> **Note:** `// SAFETY:` in test code (`#[cfg(test)]`) is not required but is encouraged for
> clarity.

### `unsafe` blocks

Every `unsafe` block must have a `// SAFETY:` comment explaining the invariants that make the
unsafe operation sound.

---

## SQL Identifier Safety

All dynamic SQL identifiers (table names, schema names) must be validated with
`validate_identifier()` (extension code) or `validate_relay_identifier()` (relay binary) before
interpolation.  Never interpolate user-supplied strings into `format!()` SQL queries without
prior validation.

## Required Tests And Flakes

`tests/required-tests.toml` is the authoritative inventory of PR, scheduled,
and release-required checks. Each entry names its workflow job, command,
targets, dependencies, owner, retry limit, and evidence path. Renaming,
removing, or replacing a required test requires an inventory change in the
same pull request and a written coverage or disposition decision.

Run the inventory checks locally:

```bash
just check-required-tests
just check-flakes
```

The flake registry at `tests/flake-registry.toml` is empty until a real
exception is reviewed. An active exception must identify an owner, issue,
sanitized failure signature, severity, release impact, first and last observed
dates, and an expiry. P0/P1 and release-blocking tests cannot be quarantined;
expired entries fail validation. The required-test wrapper still runs a flaky
test and records the original failure. It never retries a test or treats a
quarantine as a pass. Infrastructure retries, when permitted by a workflow,
must remain visible in the execution artifact. The registry must be empty
before `v1.0.0-rc.1`.

---

## Submitting Changes

1. Run `just fmt lint lint-expect test-unit` locally before opening a PR.
2. Ensure the CHANGELOG.md entry is present for any user-visible change.
3. If adding a new SQL migration, update `CHANGELOG.md`, `pg_tide.control`, and `lib.rs`.
