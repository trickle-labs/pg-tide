# Production-supported definition

A pg_tide component is production-supported only when all of these are true:

1. Its user-facing behavior is implemented and documented.
2. Its owner and security contact are recorded in the release registry.
3. The production build includes it intentionally.
4. Current contract, integration, and end-to-end evidence covers its real
   protocol boundary, including authentication, TLS, redaction, duplicate,
   failure-before/after-publish, restart, metrics, runbook, and upgrade
   evidence where applicable.
5. The supported PostgreSQL 18 path and release packaging exercise it.

“Builds successfully” and “has a unit test” are not sufficient evidence.
Anything missing one of these requirements is preview or experimental and is
not a production-support claim. The release checklist shows missing criteria;
the maturity gate is downgraded rather than weakened.

The public NATS path in `public_api_outbox_to_nats_e2e.rs` is the authoritative
end-to-end proof. Direct SDK, emulator, request-shape, database-only, and
microbenchmark tests use narrower taxonomy labels and do not substitute for it.
