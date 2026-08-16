# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for pg_tide.

An ADR captures the key architectural choices made during the project,
including the context, the decision, and the consequences.

## Records

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](adr-001-single-table-outbox.md) | Single-Table Outbox Design | Accepted |
| [ADR-002](adr-002-advisory-lock-coordination.md) | Advisory Lock Coordination | Accepted |
| [ADR-003](adr-003-wire-format-abstraction.md) | Wire Format Abstraction | Accepted |
| [ADR-004](adr-004-jsonb-catalog-config.md) | JSONB Catalog Config | Accepted |
| [ADR-005](adr-005-feature-gated-binary.md) | Feature-Gated Binary | Accepted |
| [ADR-006](adr-006-outbox-table-partitioning.md) | Outbox Table Partitioning | Accepted |
| [ADR-007](adr-007-shared-partition-table-semantics.md) | Shared Partition Table Semantics | Accepted |
| [ADR-008](adr-008-claim-check-native-pathway.md) | Native Claim-Check Pathway via pg_largeobject | Accepted |
| [ADR-009](adr-009-wal-logical-replication-source.md) | WAL Logical-Replication Source | Accepted |
| [ADR-010](adr-010-envelope-encryption-kms.md) | Envelope Encryption with KMS | Accepted |
| [ADR-011](adr-011-canonical-outbox-storage-and-relay-polling.md) | Canonical Outbox Storage and Relay Polling | Accepted |
| [ADR-012](adr-012-relay-delivery-acknowledgment-and-offset-state-machine.md) | Relay Delivery Acknowledgment and Offset State Machine | Accepted |
| [ADR-013](adr-013-retention-partitioning-and-postgresql-cost.md) | Retention, ID-Range Partitioning, and PostgreSQL Cost Contract | Accepted |
