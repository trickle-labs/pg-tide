# v1 scope

v1.0.0 is not a feature promise for every connector or every design listed in
older planning documents. It will be defined only after the focused v0.x
support policy is proven and the project publishes a reviewed v1 plan.

## In scope for the supported foundation

- Transactional PostgreSQL outbox.
- Idempotent PostgreSQL inbox.
- Relay catalog configuration and the documented `core` relay path.
- Observable delivery, retry, and failure behavior covered by the release
  evidence.

## Explicitly outside the current promise

- Unproven connector ecosystems and protocol adapters.
- PostgreSQL 17 support until its complete feasibility gate passes.
- A stable plugin ABI, broad wire-format compatibility, or a promise that all
  compiling feature combinations are production-ready.
- Historical v1 features listed in archived roadmaps without current
  implementation and evidence.

The [support policy](support/support-policy.md),
[production-supported definition](support/production-supported.md), and
[focused roadmap](../../plans/pg-tide-roadmap-to-focused-production-grade.md)
are the authoritative boundaries for v0.41.0.

