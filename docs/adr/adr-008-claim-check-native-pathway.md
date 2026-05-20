# ADR-008: Native Claim-Check Pathway via pg_largeobject

**Status:** Accepted  
**Date:** 2025  
**Deciders:** pg-tide maintainers  

---

## Context

Outbox messages occasionally exceed the practical size limit for a JSONB payload column (typically measured in megabytes, though PostgreSQL supports up to 1 GB per TOAST row). Storing very large binary blobs directly in the outbox table causes excessive table bloat, increases WAL volume, and slows down polling queries that scan all rows.

A common pattern in message-oriented systems is the *claim-check* pattern: instead of embedding the payload inline, store it in a separate object store and embed a reference (the "check") in the message.

PostgreSQL ships with a built-in binary large object facility, `pg_largeobject`, which is transactionally consistent with the main database. Using it for the claim-check store avoids introducing an external dependency (S3, GCS, etc.) for deployments that already want all data in PostgreSQL.

---

## Decision

We implement a *native claim-check pathway* using `pg_largeobject`:

1. **`tide.outbox_publish_large(name, payload, dedup_key, threshold_bytes)`** — A new SQL function that:
   - If `octet_length(payload::text) > threshold_bytes`, writes the JSON payload into a large object via `lo_from_bytea(0, …)` and inserts a claim-check envelope `{"_cc": true, "oid": "<loid>"}` into the outbox table.
   - Otherwise, falls back to the normal `tide.outbox_publish()` path.

2. **Relay source reads** (`pg-tide-relay/src/source/outbox.rs`):
   - In `decode_payload()`, when `raw_mode = true` and the payload contains `{"_cc": true, "oid": "<loid>"}`, the relay fetches the actual payload via `SELECT lo_get($1)` and replaces the envelope with the real content before passing the message to the sink.
   - During `poll_simple()`, for each claim-check row the parsed `oid` (as `u32`) is recorded in `OutboxPollerSource::pending_cc_oids`.

3. **Post-ack cleanup** (`OutboxPollerSource::acknowledge()`):
   - After successfully committing the outbox offset, the relay calls `SELECT lo_unlink($1)` for every OID in `pending_cc_oids` and clears the list.
   - `lo_unlink` errors are logged at `WARN` and do not fail the ack — a dangling large object is less harmful than a stuck pipeline.

---

## Consequences

### Positive
- No external object store required for claim-check support.
- Transactionally consistent: the large object and the outbox row are in the same PostgreSQL database.
- Simple upgrade path: existing outbox tables are unaffected; `outbox_publish_large` is an additive function.
- Relay cleanup is automatic and happens right after confirmed delivery.

### Negative
- `pg_largeobject` rows are stored in a separate system catalog (`pg_largeobject` / `pg_largeobject_metadata`). They are included in `pg_dump` by default, but can surprise operators who are not aware of them.
- If the relay crashes between polling and acknowledging (i.e., after `lo_get` but before `lo_unlink`), the large object will leak. A periodic cleanup via `vacuumlo` or `tide.relay_truncate_delivery_receipts` (combined with a sweep query) is recommended for production deployments.
- The database role running the relay must have `EXECUTE` on `lo_get` and `lo_unlink`. The `doctor` subcommand (`pg-tide doctor`) now checks for these privileges.
- Consumer-group mode does not support raw payload mode (and therefore does not use this pathway). Only simple-mode pipelines with `raw_payload_mode = true` benefit from it.

---

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Configurable S3/GCS object store | Adds an external dependency; complicates single-node deployments |
| `BYTEA` overflow table | No standard mechanism; requires custom Rust + SQL; no ecosystem tooling |
| Compress-and-embed (LZ4) | Helps with compressible payloads only; does not address fundamental size limits |
| External claim-check with URL in payload | Requires the consumer (not the relay) to perform a secondary fetch; breaks the relay abstraction |

---

## References

- PostgreSQL docs: [Large Objects](https://www.postgresql.org/docs/current/largeobjects.html)
- Martin Fowler: [Claim-Check pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html)
- `sql/pg_tide--0.27.0--0.28.0.sql` — `tide.outbox_publish_large` implementation
- `pg-tide-relay/src/source/outbox.rs` — relay-side decode and cleanup
