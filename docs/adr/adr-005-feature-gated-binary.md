# ADR-005: Feature-Gated Binary

**Status:** Accepted  
**Date:** 2026-05-05  
**Author:** pg_tide Contributors  

## Context

The `pg-tide` relay binary supports 30+ optional backends (NATS, Kafka,
Redis, RabbitMQ, SQS, Google Pub/Sub, Kinesis, Azure Service Bus, MQTT,
ClickHouse, MongoDB, Snowflake, BigQuery, Apache Iceberg, Delta Lake, etc.).
Each backend adds compile-time dependencies and increases binary size.

The design question is whether to ship a single "batteries-included" binary
or to use Cargo feature flags to produce slim and full variants.

**Options evaluated:**

- **Single monolithic binary** — compile all backends unconditionally.
- **Cargo feature flags** — each backend is an optional feature; users (or
  the release workflow) select the combination they need.
- **Plugin system** — dynamic loading of backend `.so` files at runtime.

## Decision

We use **Cargo feature flags** with a well-defined default feature set.

```toml
[features]
default = ["nats", "webhook", "stdout"]
```

The release workflow builds two variants:

| Tag | Features | Use case |
|-----|----------|----------|
| `:latest` (slim) | `default` | Minimal; for NATS/HTTP/stdout use cases |
| `:latest-full` | `--all-features` | All backends; for polyglot environments |

Users who build from source pass `--features kafka,redis,sqs` to enable
exactly the backends they need.

Rationale:

1. **Binary size** — The slim build is ~15 MB; the full build including Kafka
   (`rdkafka`, which embeds librdkafka) is ~80 MB. Users who only use NATS
   should not pay the Kafka cost.
2. **Compile time** — Feature-gated builds reduce CI time for the most common
   development workflow.
3. **Security surface** — A slim binary has fewer transitive dependencies and
   a smaller vulnerability surface.
4. **Operational clarity** — The feature set is visible via
   `pg-tide --version` (future work) and compile-time constants.

## Consequences

- **Positive**: Users choose the right binary for their environment without
  forking.
- **Positive**: `cargo-deny` advisory checks cover only the enabled features,
  reducing noise.
- **Negative**: The feature matrix is combinatorially large; CI only validates
  the slim default, the full `--all-features` build, and specific platform
  exclusions (Kafka on Windows MSVC). Edge-case feature combinations could
  have build issues undiscovered until a user reports them.
- **Negative**: Documentation must clearly state which features are required
  for each backend; a mismatch produces a runtime "feature not compiled in"
  error rather than a compile error.
- **Mitigation**: The `validate-config` command detects missing feature
  compilation and reports a helpful error.
