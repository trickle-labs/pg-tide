# CloudNativePG Integration

[CloudNativePG (CNPG)](https://cloudnative-pg.io) is the most popular
PostgreSQL operator for Kubernetes. pg_tide integrates naturally with CNPG:
the relay runs as a sidecar container alongside each PostgreSQL pod, sharing the
same lifecycle and connection string.

## Quick start

See the ready-to-use manifest in
[examples/cnpg/cluster.yaml](../../../examples/cnpg/cluster.yaml).

## Custom PostgreSQL image

CNPG uses container images for PostgreSQL. To bundle pg_tide, extend the
official CNPG image:

```dockerfile
FROM ghcr.io/cloudnative-pg/postgresql:18

# Copy the compiled extension files.
COPY pg_tide.so      /usr/lib/postgresql/18/lib/
COPY pg_tide.control /usr/share/postgresql/18/extension/
COPY sql/pg_tide--0.1.0.sql /usr/share/postgresql/18/extension/
```

Build and push to your container registry, then reference it in the CNPG
`Cluster` spec:

```yaml
spec:
  imageName: ghcr.io/your-org/postgres-pg-tide:18
```

## Sidecar pattern

The relay runs as a sidecar in the same pod as PostgreSQL. It connects to
`localhost:5432` via the injected `*-app` secret that CNPG creates automatically:

```yaml
spec:
  sidecars:
    - name: pg-tide-relay
      image: ghcr.io/trickle-labs/pg-tide:0.51.0
      env:
        - name: PG_TIDE_POSTGRES_URL
          valueFrom:
            secretKeyRef:
              name: my-cluster-app    # CNPG-generated secret
              key: uri
```

Because the relay runs in the same pod, it connects over the loopback interface
with zero network latency — ideal for high-throughput workloads.

## High availability

CNPG manages primary/replica failover automatically. The relay on the primary
pod holds PostgreSQL advisory locks on each pipeline. When a failover occurs:

1. The primary pod is terminated → advisory locks are released.
2. CNPG promotes a replica to primary.
3. The relay sidecar on the new primary pod starts and acquires the locks.
4. Message delivery resumes from the last committed consumer offset.

In-flight messages from the old primary are re-delivered when their source
checkpoint was not yet advanced. A downstream publish may therefore duplicate
with the same stable event ID; it does not silently skip the event.

## Initialisation SQL

Bootstrap pg_tide only when CNPG creates the cluster. Existing clusters need
an explicit migration Job after the extension image is available:

```yaml
spec:
  bootstrap:
    initdb:
      database: app
      owner: app
      postInitSQL:
        - CREATE EXTENSION IF NOT EXISTS pg_tide;
        - CREATE ROLE relay_user LOGIN PASSWORD 'strong-password';
        - GRANT USAGE ON SCHEMA tide TO relay_user;
        - GRANT SELECT, INSERT ON tide.tide_outbox_messages TO relay_user;
        - GRANT SELECT ON ALL TABLES IN SCHEMA tide TO relay_user;
```

## Prometheus monitoring

CNPG integrates with the Prometheus Operator. Add a `ServiceMonitor` to scrape
relay metrics alongside the built-in CNPG metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: pg-tide-relay
spec:
  selector:
    matchLabels:
      cnpg.io/cluster: my-cluster
  endpoints:
    - port: relay-metrics
      path: /metrics
      interval: 15s
```
