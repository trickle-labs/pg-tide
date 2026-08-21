# ADR-014: Upgrade, rollback, and recovery contract

## Status

Accepted for v0.51.0.

## Decision

`schemas/lifecycle-compatibility-v1.json` is the single policy for supported
extension versions, relay combinations, migration behavior, rollback limits,
and recovery guarantees. `scripts/check_lifecycle_contract.py` rejects drift
between that policy and packaged files, required tests, documentation, and
release controls.

The supported extension floor is v0.47.0 and the target is v0.51.0. PostgreSQL
must apply every adjacent migration in order. The only supported rolling relay
window is v0.50.0 to v0.51.0. Relays from earlier versions stop before the
extension update; a v0.50.0 relay may be used as the post-update rollback
window.

No v0.51.0 extension downgrade is promised. A committed upgrade that must
return to v0.50.0 requires PostgreSQL restore or PITR. Failed DDL is expected to
remain transactional: before commit, correct the precondition and retry; after
commit, restore or continue forward. External destinations are outside the
PostgreSQL recovery boundary.

## Consequences

- At-least-once recovery can redeliver an event accepted before PostgreSQL was
  restored. Stable event identity and destination deduplication are required.
- Persistent pg_tide state is restored with supported logical or physical
  PostgreSQL recovery. Runtime leases and ownership are reacquired by new relay
  processes.
- The policy checker deliberately fails when a required migration, test,
  example, or v0.51.0 evidence target is absent; a green documentation check
  cannot imply that those artifacts exist.
