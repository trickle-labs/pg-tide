# Runbook: Backup and Restore

This runbook covers the v0.51.0 recovery boundary. PostgreSQL state can be
restored; an external destination is not rolled back with it.

## Logical backup

Take globals and a complete custom-format database dump before an extension
update:

```bash
pg_dumpall --globals-only --no-role-passwords > globals.sql
pg_dump --format=custom --file=pg-tide.dump "$PG_TIDE_POSTGRES_URL"
```

Restore roles first, then restore the database into a clean PostgreSQL 18
cluster with the same pg_tide extension artifact installed. Verify the
extension version, owners, grants, sequences, inbox tables, checkpoints, and
DLQ rows before starting the relay.

```bash
psql --file=globals.sql postgres
createdb restored
pg_restore --exit-on-error --dbname=restored pg-tide.dump
```

Start the relay only after `pg-tide doctor` reports a compatible extension. New
events must continue from the restored source frontier; a checkpoint committed
after the restore point must not be recreated manually.

## Physical backup and PITR

Use PostgreSQL base backups and archived WAL for physical recovery. Restore the
cluster to the required recovery point, verify `pg_extension.extversion`, and
let new relay processes reacquire ownership. Leases, advisory locks, and
heartbeats are transient and must not be treated as restored authority.

Restoring before a checkpoint commit may redeliver an event. This is allowed
at-least-once behavior. The event identity remains stable and the destination
must apply its documented deduplication rule. pg_tide does not claim to roll
back NATS, Kafka, or webhook effects.

## Choosing the recovery action

- Use relay rollback only to run v0.50.0 against a v0.51.0 extension.
- Use an exact reverse extension migration only where the lifecycle policy marks
  the step reversible.
- Use database restore or PITR for the irreversible v0.50.0 → v0.51.0 boundary.

Never repair `pg_extension` or `tide` tables by hand.
