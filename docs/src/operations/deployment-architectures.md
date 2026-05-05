# Deployment Architectures

This guide covers proven deployment patterns for pg_tide in production, from simple single-instance setups to globally distributed architectures.

## Single Instance

The simplest deployment: one relay process connected to one PostgreSQL database.

```
┌─────────────┐       ┌────────────┐       ┌──────────┐
│ Application │──────→│ PostgreSQL │──────→│ pg-tide  │──→ Sinks
│  (INSERT)   │       │  (outbox)  │       │ (relay)  │
└─────────────┘       └────────────┘       └──────────┘
```

**When to use:** Development, staging, low-throughput production (< 10,000 msg/s), applications where simplicity matters more than redundancy.

**Pros:** Simple to operate, no coordination overhead, easy to debug.

**Cons:** Single point of failure. If the relay crashes, messages queue in the outbox until it restarts.

## Active-Active High Availability

Multiple relay instances share the workload using PostgreSQL advisory locks for coordination.

```
┌──────────┐     ┌────────────┐     ┌───────────┐
│ pg-tide  │────→│            │     │           │
│ relay #1 │     │            │────→│   Sinks   │
└──────────┘     │ PostgreSQL │     │           │
                 │            │     └───────────┘
┌──────────┐     │            │
│ pg-tide  │────→│            │
│ relay #2 │     └────────────┘
└──────────┘
```

Each instance acquires advisory locks for pipelines. If instance #1 crashes, instance #2 picks up its pipelines within one discovery interval (default: 30 seconds).

**When to use:** Production workloads requiring fault tolerance.

**Configuration:**
```bash
# Both instances use the same relay-group-id
pg-tide --relay-group-id "production" --discovery-interval 10
```

**Pros:** Automatic failover, no manual intervention, pipelines distributed across instances.

**Cons:** Slightly more complex to deploy and monitor.

## Kubernetes Deployment

The recommended production deployment for most teams. pg_tide runs as a Kubernetes Deployment with multiple replicas.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-tide-relay
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pg-tide-relay
  template:
    metadata:
      labels:
        app: pg-tide-relay
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: pg-tide
          image: ghcr.io/your-org/pg-tide:latest
          args:
            - --postgres-url
            - $(DATABASE_URL)
            - --relay-group-id
            - production
            - --shutdown-timeout
            - "45"
          ports:
            - containerPort: 9090
              name: metrics
          livenessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 5
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: pg-tide-secrets
                  key: database-url
```

**Key considerations:**
- Set `terminationGracePeriodSeconds` > `shutdown-timeout`
- Use `readinessProbe` on `/health` to remove unhealthy pods from service discovery
- Store secrets in Kubernetes Secrets or external secret managers
- Use `PodDisruptionBudget` to prevent all replicas from being evicted simultaneously

## Sidecar Pattern

Run pg_tide as a sidecar container alongside your application in the same pod. The relay connects to the same database and handles message delivery.

```yaml
spec:
  containers:
    - name: app
      image: your-app:latest
    - name: pg-tide
      image: ghcr.io/your-org/pg-tide:latest
      args: ["--postgres-url", "$(DATABASE_URL)"]
```

**When to use:** When each service has its own dedicated outbox and you want relay lifecycle tied to the application.

**Pros:** Simple lifecycle management, dedicated resources per service.

**Cons:** More total relay instances, each handling fewer pipelines.

## Multi-Region

For globally distributed applications, run relay instances in each region connecting to local read replicas or regional databases.

```
Region US:                    Region EU:
┌──────────┐                  ┌──────────┐
│ pg-tide  │──→ US Sinks      │ pg-tide  │──→ EU Sinks
│ relay    │                  │ relay    │
└──────────┘                  └──────────┘
     │                             │
     ↓                             ↓
┌──────────┐                  ┌──────────┐
│ PG (US)  │ ←── replication ──→ │ PG (EU) │
└──────────┘                  └──────────┘
```

**Key considerations:**
- Use different `relay-group-id` per region to prevent cross-region lock contention
- Or use the same group ID with a shared database for automatic geographic failover
- Consider latency to sinks when choosing relay placement

## Further Reading

- [HA Coordination](../features/ha-coordination.md) — Advisory lock mechanics
- [Graceful Shutdown](../features/graceful-shutdown.md) — Kubernetes shutdown integration
- [Scaling](scaling.md) — Throughput optimization
- [CloudNativePG Integration](../integration/cloudnativepg.md) — PostgreSQL operator setup
