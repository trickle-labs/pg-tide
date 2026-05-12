# Runbook: Relay Upgrade

**Applies to:** pg-tide relay binary (pg-tide run)  
**Scope:** Rolling upgrade procedure for high-availability deployments with
multiple relay instances.

---

## Overview

The pg-tide relay is stateless between reconcile cycles.  Pipeline ownership
is coordinated via PostgreSQL advisory locks, so multiple relay instances can
run simultaneously without split-brain.  This makes rolling upgrades possible
without any downtime to message delivery.

---

## Pre-Upgrade Checklist

1. **Read the CHANGELOG** for the target version — note any new required
   configuration keys or deprecated flags.
2. **Back up the database** (or confirm PITR is current).
3. **Confirm current relay health:**
   ```bash
   pg-tide status --postgres-url "$PG_TIDE_POSTGRES_URL"
   pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"
   ```
4. **Upgrade the PostgreSQL extension first** (if the target relay version
   requires a newer extension schema):
   ```sql
   ALTER EXTENSION pg_tide UPDATE;
   ```
   The old relay is forward-compatible with the new schema; the new relay is
   backward-compatible with the old schema.  Upgrading the extension first
   is always safe.

---

## Rolling Upgrade Procedure (Kubernetes)

### 1. Update the Deployment Image Tag

```bash
kubectl set image deployment/pg-tide \
  pg-tide=ghcr.io/trickle-labs/pg-tide:0.19.0
```

Or update `image.tag` in `values.yaml` and run `helm upgrade`:

```bash
helm upgrade pg-tide oci://ghcr.io/trickle-labs/helm/pg-tide \
  --set image.tag=0.19.0 \
  --reuse-values
```

### 2. Watch the Rollout

```bash
kubectl rollout status deployment/pg-tide
```

Kubernetes replaces pods one at a time (controlled by `maxUnavailable` and
`maxSurge`).  As each old pod is terminated:

1. PostgreSQL advisory locks held by the old pod are **released automatically**
   when the connection closes (within ~1 s of pod termination).
2. The new pod's coordinator reconciles and reacquires the released pipelines.
3. Messages may be re-delivered (at-least-once) for any batch that was
   in-flight at the time of the old pod's termination.

### 3. Verify Post-Upgrade

```bash
# All pods should be running the new version:
kubectl get pods -l app.kubernetes.io/name=pg-tide -o json \
  | jq '.items[].spec.containers[].image'

# Pipeline ownership should be fully restored:
pg-tide status --postgres-url "$PG_TIDE_POSTGRES_URL"
```

---

## Rolling Upgrade Procedure (Docker / Systemd)

For single-instance or manual deployments:

1. **Start the new relay** alongside the old one (different container name or
   port is fine; both will contend for advisory locks and share pipeline
   ownership gracefully).

   ```bash
   docker run -d --name pg-tide-new \
     -e PG_TIDE_POSTGRES_URL="$PG_TIDE_POSTGRES_URL" \
     ghcr.io/trickle-labs/pg-tide:0.19.0
   ```

2. **Verify the new relay is healthy** and has acquired pipelines:
   ```bash
   docker logs pg-tide-new | grep "acquired lock"
   pg-tide status --postgres-url "$PG_TIDE_POSTGRES_URL"
   ```

3. **Stop the old relay** (graceful drain):
   ```bash
   docker stop --time 60 pg-tide-old
   ```
   The `--time 60` gives the old relay up to 60 seconds to finish in-flight
   batches before hard-stopping.

4. Clean up:
   ```bash
   docker rm pg-tide-old
   docker rename pg-tide-new pg-tide
   ```

---

## Configuration Changes Between Versions

### New Required Configuration Keys

Check the CHANGELOG for any new **required** keys.  The relay will fail to
start with a clear error message if a required key is missing.

### Deprecated Flags

Deprecated flags continue to work until the next major version.  A
`WARN`-level log entry is emitted at startup if a deprecated flag is present.

### Environment Variable Changes

| Old (pre-0.17.0) | New | Notes |
|-------------------|-----|-------|
| `PG_TIDE_RELAY_POSTGRES_URL` | `PG_TIDE_POSTGRES_URL` | Old name no longer recognised |
| Pre-v0.17 legacy env var | `PG_TIDE_POSTGRES_URL` | See CHANGELOG v0.17.0 for details |

---

## Rollback

If the new relay is unhealthy after the upgrade:

1. Start the old relay binary (advisory locks auto-transfer back).
2. Stop the new relay.
3. Investigate logs from the new relay for the root cause.

Because pipeline state lives in PostgreSQL, no data is lost during rollback.

---

## HA Considerations

- **Minimum two relay instances** are recommended for production to ensure
  zero-downtime upgrades.
- **`maxUnavailable: 0`** in the Kubernetes PodDisruptionBudget ensures at
  least one relay is always running during node drains.
- The Helm chart defaults (`helm/pg-tide/values.yaml`) set
  `replicaCount: 2` and include a PodDisruptionBudget.

---

## See Also

- [Schema Migration runbook](runbook-schema-migration.md)
- [Crash Recovery runbook](runbook-crash-recovery.md)
- [Deployment Architectures](deployment-architectures.md)
