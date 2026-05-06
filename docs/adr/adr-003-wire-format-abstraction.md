# ADR-003: Wire Format Abstraction

**Status:** Accepted  
**Date:** 2026-05-05  
**Author:** pg_tide Contributors  

## Context

The relay binary needs to speak multiple message envelope formats:

- **Native pg_tide** (`v:1` envelope) — the default for pg_tide-to-pg_tide relay.
- **Debezium JSON** — to interoperate with Debezium consumers (Kafka Connect,
  Iceberg, Pinot, Druid, ksqlDB, Flink CDC).
- **Maxwell** and **Canal** — MySQL CDC change event formats used by legacy
  pipelines migrating to PostgreSQL.
- **CloudEvents** (v1.0) — the CNCF standard for event metadata.
- **Custom CDC JSON** — arbitrary user-supplied JSON path expressions.

The design question is whether to hard-code format handling inside each
source/sink, or to introduce a symmetric trait-based abstraction.

## Decision

We introduced the `WireFormat` trait with two symmetric operations:

```rust
trait WireFormat: Send + Sync {
    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError>;
    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError>;
}
```

The `from_config(pipeline_config)` factory function reads the `wire_format`
key from the pipeline JSON config and returns the appropriate boxed
implementation. Format selection is a pipeline-level concern, not a
source/sink-level concern.

Rationale:

1. **Decoupling** — Sources and sinks operate on `RawMessage` / `EncodedBatch`
   bytes; they are unaware of the envelope format.
2. **Symmetric** — The same trait handles both the reverse (consume) and
   forward (produce) paths, enabling bidirectional Debezium support.
3. **Extensibility** — New formats can be added by implementing the trait;
   no changes to source/sink code are required.
4. **Feature-gating** — Heavyweight formats (Maxwell, Canal, CDC-JSON) are
   optional Cargo features to keep binary size small.

## Consequences

- **Positive**: Clear extension point; third-party format plugins are a
  natural next step (WASM transforms in v1.2).
- **Positive**: Property-based round-trip tests can validate any format
  without transport setup.
- **Negative**: The coordinator must instantiate a `WireFormat` per pipeline
  and carry it through the poll loop; for stateful formats (schema registries)
  this implies a mutable component in the worker.
- **Negative**: The `BoxedWireFormat = Box<dyn WireFormat>` indirection has
  a small per-message virtual dispatch cost; acceptable at current throughputs.
