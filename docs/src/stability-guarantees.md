# Stability guarantees

v0.41.0 is a focused pre-1.0 release. It provides a supported PostgreSQL 18
path, but it does not make a blanket v1 API or connector stability promise.

## Covered by the v0.41 policy

- Documented SQL behavior on PostgreSQL 18, including transactional outbox and
  idempotent inbox operations.
- Catalog migrations shipped for the supported upgrade path.
- The matching v0.41.x extension and `pg-tide` relay release.
- Production behavior included in the `core` build profile and backed by the
  release evidence for that profile.

Bug-fix releases may correct behavior, security issues, and documentation while
preserving the documented contract. They may not silently turn preview or
experimental connectors into production guarantees.

## Not guaranteed

- PostgreSQL versions other than 18, including PostgreSQL 17.
- Experimental or preview connectors, `core-kafka`, and `experimental-full`.
- Internal Rust types, undocumented configuration keys, and generated build
  details.
- Exact metrics, logging, error text, or migration statement layout unless the
  relevant reference explicitly documents them.

See [the support policy](support/support-policy.md) and [the production
definition](support/production-supported.md) for the evidence boundary.

