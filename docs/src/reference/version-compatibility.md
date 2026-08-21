# Version compatibility

The v0.51.0 lifecycle policy is the machine-readable source of truth:
[`lifecycle-compatibility-v1.json`](../../../schemas/lifecycle-compatibility-v1.json).
The checker compares this matrix with packaged migrations, the required-test
inventory, release controls, examples, evidence, and this page.

## Supported lifecycle matrix

<!-- BEGIN LIFECYCLE MATRIX -->
| Extension | Relay | Status | Required test |
|---|---|---|---|
| 0.50.0 | 0.50.0 | supported | `lifecycle-adjacent-pr` |
| 0.50.0 | 0.51.0 | supported | `lifecycle-compatibility-pr` |
| 0.51.0 | 0.50.0 | supported | `lifecycle-adjacent-pr` |
| 0.51.0 | 0.51.0 | supported | `lifecycle-compatibility-pr` |
| 0.50.0 | mixed-0.50.0-0.51.0 | supported | `lifecycle-adjacent-pr` |
| 0.51.0 | mixed-0.50.0-0.51.0 | supported | `lifecycle-adjacent-pr` |
| <0.50.0 | 0.51.0 | rejected | `lifecycle-compatibility-pr` |
| >0.51.0 | 0.51.0 | rejected | `lifecycle-compatibility-pr` |
<!-- END LIFECYCLE MATRIX -->

The supported production extension floor is v0.47.0. Upgrade it through every
adjacent migration to v0.51.0. The rolling relay window is v0.50.0 and v0.51.0;
operators upgrading from v0.47.0 through v0.49.0 must stop the relay, upgrade
the extension, then start the target relay.

## Rollback and recovery

The v0.50.0 relay may run during the bounded v0.51.0 rolling window and may be
restored after the v0.51.0 extension update. The extension itself is not
downgraded by a reverse migration after v0.51.0: restore a backup or use PITR
to return PostgreSQL to an earlier committed state.

PostgreSQL recovery does not roll back NATS, Kafka, or webhook destinations.
Restoring before a checkpoint commit may redeliver an already accepted event;
delivery remains at-least-once. Stable event identities and destination-side
deduplication are required. Restored relays reacquire leases and resume from
the restored checkpoint frontier.

Do not edit PostgreSQL system catalogs or `tide` tables to repair a lifecycle
transition. If an upgrade fails before commit, retry after correcting the
precondition. After commit, restore or continue forward according to the
recorded migration policy.
