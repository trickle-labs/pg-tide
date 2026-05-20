# Pre-GA Readiness Checklist

> **Purpose:** Formal acceptance gate for declaring v1.0.0 Production GA.  
> Complete every item and record the result before cutting the GA tag.

---

## 1. TLS Configuration

- [ ] Verify `sslmode` is set appropriately for your PostgreSQL deployment:
  - Development: `sslmode=disable` or `sslmode=prefer` (acceptable)
  - Staging/Production: `sslmode=require` or `sslmode=verify-full` (required)
- [ ] If using `sslmode=require`, compile with `--features native-tls` or use the `:latest-full` Docker image.
- [ ] Run `pg-tide doctor` and confirm: `[OK] TLS connection: TLSv1.2` or `TLSv1.3`.
- [ ] Cloud-managed PostgreSQL services (RDS, Cloud SQL, Azure Database) behind a TLS proxy work without the `native-tls` feature.

## 2. Outbox Partitioning Strategy

- [ ] Evaluate your event throughput:
  - **< 100k events/day**: `partition_strategy = 'none'` (default) is sufficient.
  - **100k–10M events/day**: `partition_strategy = 'daily'` recommended.
  - **> 10M events/day**: `partition_strategy = 'hourly'` (future) or `'daily'` with short retention.
- [ ] For partitioned outboxes, set `retention_partitions` based on your SLA window:
  - 7 days: `retention_partitions = 7` (default for daily strategy)
  - 30 days: `retention_partitions = 30`
- [ ] Schedule `pg-tide sweep` to run at least once per partition interval to create future partitions.
- [ ] Run `pg-tide doctor` and confirm no partition capacity warnings.

## 3. Consumer Group Setup

- [ ] Register consumer groups for each pipeline using `tide.consumer_group_create()`.
- [ ] Verify committed offsets advance after test message delivery.
- [ ] Confirm `tide.poll_outbox()` correctly prunes partitions via `WHERE id > $last_offset`.
- [ ] For multi-tenant deployments: verify `tenant_name` is set correctly in `relay_outbox_config`.

## 4. DLQ Monitoring Thresholds

- [ ] Set `--dlq-warn-threshold` (default: 100 per hour) appropriate for your data quality SLA.
- [ ] Confirm `tide.relay_dlq` has an alert in your monitoring stack for entries older than your max retry window.
- [ ] Test DLQ replay procedure: run `pg-tide replay dlq-requeue` against a staging deployment.
- [ ] Verify `pg_tide_relay_dlq_entries_written_total` metric is visible in your Prometheus/Grafana stack.

## 5. `pg-tide doctor` Output Interpretation

Run `pg-tide doctor --postgres-url $PG_TIDE_POSTGRES_URL` and verify all lines are `[OK]`:

```
pg-tide doctor v0.25.0
  [OK] Connected to PostgreSQL
  [OK] TLS connection: TLSv1.3
  [OK] Schema 'tide' exists
  [OK] Table tide.tide_outbox_config
  [OK] Table tide.relay_outbox_config
  ...
  [OK] DLQ hourly rate: 0 (threshold: 100)
  [OK] relay_consumer_offsets.last_change_id column present
  [OK] Advisory lock acquisition succeeded
  [OK] LISTEN on tide_relay_config permitted

pg-tide doctor: all checks passed.
```

**Expected warnings (non-blocking):**
- `[INFO] DuckLake catalog tables not found` — expected unless DuckLake is in use.
- `[INFO] Connection is plaintext` — acceptable in private-network deployments.

**Blocking failures (must resolve before GA):**
- `[FAIL] Schema 'tide' not found`
- `[FAIL] Table tide.relay_dlq missing`
- `[FAIL] LISTEN on tide_relay_config denied`

## 6. Helm Security Context Review

- [ ] `runAsNonRoot: true` and `runAsUser: 1000` are set in the Deployment spec.
- [ ] `readOnlyRootFilesystem: true` is set (requires `/tmp` to be a writable emptyDir volume).
- [ ] `allowPrivilegeEscalation: false` is set.
- [ ] `PodDisruptionBudget` is configured with `minAvailable: 1` for HA deployments.
- [ ] `ServiceMonitor` is configured and Prometheus is scraping `/metrics`.

## 7. Benchmark Baseline Validation

- [ ] Run `just bench` and confirm no regressions vs the committed baseline.
- [ ] `outbox_poll_decode` at 1kb batch: baseline < 5ms.
- [ ] `inbox_unnest_params` at 1000 rows: baseline < 1ms.
- [ ] `worker_inner_orchestration` at 1000 messages: baseline < 10ms.

If benchmarks regress by more than 10%, investigate before declaring GA.

## 8. Rollback Procedure

If a v1.0.0 deployment needs to be rolled back to v0.25.0:

1. Stop the relay: `kubectl rollout undo deployment/pg-tide-relay`
2. Verify no schema changes are breaking: `pg-tide doctor` should pass with the v0.25.0 schema.
3. If the outbox was converted to partitioned: the old backup table is preserved at  
   `tide.tide_outbox_messages_backup_{outbox_name}` — no rollback SQL needed.
4. All consumer offsets are preserved: polling resumes from the last committed offset.

---

## Acceptance Sign-off

| Category | Status | Notes |
|----------|--------|-------|
| TLS configuration | ☐ | |
| Outbox partitioning | ☐ | |
| Consumer group setup | ☐ | |
| DLQ monitoring | ☐ | |
| `pg-tide doctor` clean | ☐ | |
| Helm security contexts | ☐ | |
| Benchmark baseline | ☐ | |
| Rollback procedure tested | ☐ | |

Once all items are checked, cut the v1.0.0 tag.

---

## v1.0.0 GA Acceptance Criteria (from assessment cycle 6)

The following items must be resolved before v1.0.0 GA.  Track resolution version
in the **Resolved in** column.

### Must-do (P0 blockers)

| Item | Description | Resolved in |
|------|-------------|-------------|
| Stability contract documented | `docs/src/stability-guarantees.md` published | v0.33.0 |
| KMS envelope encryption foundation | ADR-010 written, SQL schema added, feature-gated Rust trait skeleton | v0.33.0 |
| Positional SQL APIs deprecated | `relay_set_outbox` / `relay_set_inbox` 6/8-param forms emit SQL `NOTICE` | v0.30.0 |
| v0.x → v1.0.0 migration guide | Comprehensive rolling-upgrade guide with rollback procedure | v0.33.0 |
| supply-chain hardening | `cargo audit` suppress list re-evaluated; all 9 entries confirmed optional-dep only | v0.33.0 |
| `--expect-extension-version` flag | Relay pre-flight can gate on minimum SQL extension version | v0.33.0 |

### Should-do (P1 polish)

| Item | Description | Resolved in |
|------|-------------|-------------|
| Inbox fleet dashboard panel | Grafana panel for `tide.inbox_status(NULL)` added to relay-health.json | v0.33.0 |
| `pg-tide status --inbox-summary` | Fleet inbox summary in status CLI output | v0.33.0 |
| `just check-stability` recipe | Automated check that metric names and schema annotations are stable | v0.33.0 |
| KMS encryption implementation | Actual encryption logic for AwsKms, GcpKms, VaultKms, LocalKeyFile | v1.0.0 |
| Positional SQL APIs removed | Drop the deprecated 6/8-param forms | v1.0.0 |
