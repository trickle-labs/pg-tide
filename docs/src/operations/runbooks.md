# Operations runbooks

Use the canonical commands first. They are read-only unless explicitly stated:

```bash
pg-tide doctor --output json
pg-tide status --output json
pg-tide config validate --output json
```

| Symptom / alert | Runbook section | First command | Evidence |
|---|---|---|---|
| Relay will not start | [Relay will not start](#relay-will-not-start) | `pg-tide doctor --output json` | doctor envelope and pod logs |
| Pipeline not discovered | [Pipeline is not discovered](#pipeline-is-not-discovered) | `pg-tide status --output json` | status ownership/config result |
| Lag alert | [Pipeline has lag](#pipeline-has-lag) | `pg-tide status --output json` | lag, retry, latency trends |
| Publish errors | [Sink authentication failure](#sink-authentication-failure) | `pg-tide doctor --output json` | redacted connector check |
| Bounded connector failures | [Sink authentication failure](#sink-authentication-failure) and the connector runbooks | `pg-tide status --output json` | code, retry class, and connector metric |
| DLQ alert | [DLQ is growing](#dlq-is-growing) | `pg-tide status --output json` | unresolved depth and write rate |
| Ownership unclear | [Advisory lock is stuck](#advisory-lock-is-stuck-or-ownership-is-unclear) | `pg-tide doctor --output json` | PostgreSQL session evidence |
| PostgreSQL failover | [PostgreSQL failover occurred](#postgresql-failover-occurred) | `pg-tide status --output json` | primary, owner, checkpoint |
| Retention blocked | [Retention is not cleaning up](#retention-is-not-cleaning-up) | `pg-tide maintenance sweep --dry-run` | sweep report |
| Disk growth | [Disk usage is growing](#disk-usage-is-growing) | `pg-tide status --output json` | measured storage breakdown |
| Upgrade failure | [Upgrade failed](#upgrade-failed) | `pg-tide doctor --output json` | migration/job and version matrix |
| Duplicates | [Duplicate messages](#duplicate-messages-are-observed) | `pg-tide status --output json` | stable event IDs and ownership |

## Relay will not start

Run `doctor --output json`; do not bypass validation. Check parse/unknown-key errors, missing URL or secret, TLS/authentication, extension compatibility, role grants, enabled pipeline validation, and port binding. Fix the reported component and rerun doctor. Keep relay stopped on validation failure. If migration state is uncertain, preserve logs and backup evidence and escalate before rollback.

## Pipeline is not discovered

Check group, tenant, direction, enabled state, catalog visibility/grants, structural config, compiled connector feature, reconcile delay, and current owner with `status` and `doctor`. Do not insert or update catalog rows directly. Correct configuration through the supported SQL API, then wait for reconciliation and verify ownership.

## Pipeline has lag

Compare arrival, publish, consumer lag, delivery latency, retry state, pool waiting, ownership, and cleanup/storage trends. A single threshold cannot identify the cause. Reduce source pressure or correct the sink; scale only after confirming ownership distribution and configured batch/concurrency bounds. Capture before/after status evidence.

## Sink authentication failure

Verify secret-reference existence, file ownership/mode, certificate hostname and expiry, destination authorization, and connector configuration. Use the redacted doctor probe; never print secrets or disable TLS verification. Rotate credentials, then verify publish recovery and error-rate resolution.

## DLQ is growing

Separate unresolved depth from write rate and classify errors using payload-safe inspection. Confirm DLQ persistence is healthy, check idempotency and duplicate effects, and use the supported replay preview/execute flow. Verify replay does not mutate the live checkpoint unexpectedly.

## Advisory lock is stuck or ownership is unclear

Treat owner, stale, and unknown as different states. Inspect PostgreSQL sessions and heartbeat evidence; drain safely and terminate only the identified stale session through the documented operational procedure. Failover releases session locks. Do not delete status rows or unlock arbitrary sessions.

## PostgreSQL failover occurred

Confirm the new primary, connectivity/TLS, extension availability, and relay readiness. Verify old ownership sessions are gone, then observe advisory-lock reacquisition, checkpoints, lag drain, and expected at-least-once duplicates. Escalate immediately if two owners appear.

## Retention is not cleaning up

Run the public sweep dry-run and inspect participant blockers, disabled pipelines, checkpoints, active leases, cleanup freshness, partition blockers, and rewind implications. Apply bounded cleanup through public maintenance APIs; never delete rows manually.

## Disk usage is growing

Break usage into retained messages, DLQ, indexes/dead tuples, WAL, partitions, logs, and container filesystem. Use capacity guidance and measured evidence before changing retention. Apply bounded cleanup and escalate if WAL or filesystem growth is unrelated to message retention.

## Upgrade failed

Classify failure: before migration, during migration, after extension/before relay, mixed relay rollout, relay replacement, CNPG job, or rollback. Preserve backup and job logs. Continue publishing only when the compatibility matrix says it is safe; otherwise pause relay, use the documented reverse migration only when eligible, or restore/PITR. Never assume bootstrap hooks migrated an existing cluster.

## Duplicate messages are observed

Use stable event identity to distinguish expected publish-ack/checkpoint ambiguity, ownership transfer, replay/rewind, and destination duplication. Confirm only one owner at a time. At-least-once delivery means duplicates can be expected; do not claim recovery can eliminate all duplicates.
