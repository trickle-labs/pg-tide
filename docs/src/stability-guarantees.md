# Stability guarantees

The v0.47.0 policy is a public-beta contract freeze, not a v1.0.0 GA
promise. It freezes documented, production-supported behavior and keeps
preview, experimental, diagnostic, and internal surfaces outside the v1
guarantee.

## Frozen public surfaces

The release candidate freezes documented PostgreSQL 18 SQL APIs, the
supported pipeline configuration subset, core Prometheus metrics and health
endpoints, versioned CLI machine-readable output, native pg_tide and
CloudEvents envelopes, and the direction-aware support matrix.

Each surface has normative documentation and executable or machine-checkable
evidence. Ownership, maturity, versions, profiles, and evidence come from
[`connectors.toml`](../../connectors.toml), not compilation alone.

## Compatibility rules

Removing or renaming a documented field, endpoint, function, metric family,
envelope field, supported connector, or supported version is breaking. So are
changes to types, requiredness, defaults, status semantics, acknowledgement
boundaries, deduplication identity, or documented meaning.

Additive optional fields, bounded metric families, separate endpoints, and new
compatible service versions require compatibility review and documented
defaults. Security and correctness fixes may change behavior when they
preserve the documented safety invariant and include migration guidance.
Internal Rust layout, log wording, migration statement order, and undocumented
configuration remain outside the guarantee.

## Explicit exclusions

There is no v1 guarantee for PostgreSQL versions other than 18, inbound
connectors, `experimental-full`, preview or experimental connectors, a stable
Rust/plugin ABI, arbitrary source-to-sink combinations, or exactly-once
transport. See the [support policy](support/support-policy.md) and
[production-supported definition](support/production-supported.md).
