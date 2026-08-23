# Deployment

This page covers everything you need to deploy pg_tide in production: from a single-machine setup to highly-available Kubernetes deployments. pg_tide has two components to deploy — the PostgreSQL extension and the relay binary — and both are designed to be operationally simple.

---

## Components Overview

| Component | What it is | Where it runs | State |
|-----------|-----------|---------------|-------|
| **pg_tide extension** | SQL functions + catalog tables | Inside your PostgreSQL database | All state in PostgreSQL tables |
| **pg-tide relay** | Standalone binary that bridges messages | Anywhere with network access to PostgreSQL + sinks | Stateless — all state in PostgreSQL |

The relay binary is completely stateless. You can kill it, restart it, replace it, scale it up or down — it always recovers from the last committed offset stored in PostgreSQL. This makes deployment and upgrades straightforward.

---

## Extension Deployment

Install the extension on your PostgreSQL 18 server:

```sql
CREATE EXTENSION pg_tide;
```

The extension creates the `tide` schema with all required tables, views, triggers, and functions. It requires no background workers, no shared memory, and no file system access — making it compatible with:

- All managed PostgreSQL services (RDS, Cloud SQL, Azure Database, Supabase, Neon)
- Connection poolers (PgBouncer, PgCat, Pgpool-II)
- CloudNativePG and other Kubernetes operators
- Standard replication setups (streaming, logical)

### Permissions

The extension can be installed by any user with `CREATE` privilege on the database. No superuser required. After installation, grant appropriate permissions:

```sql
-- Application users can publish to outboxes
GRANT USAGE ON SCHEMA tide TO app_user;
GRANT EXECUTE ON FUNCTION tide.outbox_publish(text, jsonb, jsonb) TO app_user;

-- Relay user needs read/write access to message tables
CREATE ROLE pg_tide_relay LOGIN PASSWORD 'strong-password';
GRANT USAGE ON SCHEMA tide TO pg_tide_relay;
GRANT SELECT, UPDATE ON tide.tide_outbox_messages TO pg_tide_relay;
GRANT SELECT ON tide.tide_outbox_config TO pg_tide_relay;
GRANT SELECT ON tide.relay_outbox_config TO pg_tide_relay;
GRANT SELECT ON tide.relay_inbox_config TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE ON tide.tide_consumer_offsets TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON tide.tide_consumer_leases TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE ON tide.relay_consumer_offsets TO pg_tide_relay;
```

---

## Standalone Binary Deployment

The simplest deployment: download the relay binary and run it directly.

### Download and install

```bash
# Linux (amd64)
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-x86_64-unknown-linux-gnu.tar.gz
sudo mv pg-tide /usr/local/bin/

# macOS (Apple Silicon)
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-aarch64-apple-darwin.tar.gz
tar xzf pg-tide-aarch64-apple-darwin.tar.gz
sudo mv pg-tide /usr/local/bin/
```

### Run the relay

```bash
pg-tide \
  --postgres-url "postgres://pg_tide_relay:pass@db.internal:5432/app" \
  --relay-group-id production \
  --log-format json \
  --metrics-addr 0.0.0.0:9090
```

### Systemd service (Linux)

For production Linux deployments, run the relay as a systemd service:

```ini
# /etc/systemd/system/pg-tide-relay.service
[Unit]
Description=pg-tide relay
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pgtide
Group=pgtide
ExecStart=/usr/local/bin/pg-tide \
  --config /etc/pg-tide/relay.toml
Restart=always
RestartSec=5
# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pg-tide-relay
sudo systemctl start pg-tide-relay
```

---

## Docker Deployment

The official Docker image is lightweight (~20 MB, Alpine-based) and runs as a non-root user.

### Quick start

```bash
docker run -d \
  --name pg-tide-relay \
  -e PG_TIDE_POSTGRES_URL="postgres://user:pass@host.docker.internal:5432/mydb" \
  -e PG_TIDE_LOG_FORMAT="json" \
  -e PG_TIDE_GROUP_ID="production" \
  -p 9090:9090 \
  ghcr.io/trickle-labs/pg-tide:latest
```

### Image details

| Property | Value |
|----------|-------|
| Base | Alpine 3.21 |
| Size | ~20 MB |
| User | `pgtide` (UID 1000) |
| Entrypoint | `pg-tide` |
| Exposed Port | 9090 (metrics + health) |

### Environment variables

All relay configuration can be passed via environment variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `PG_TIDE_POSTGRES_URL` | PostgreSQL connection string | Yes |
| `PG_TIDE_METRICS_ADDR` | Metrics endpoint (default: `0.0.0.0:9090`) | No |
| `PG_TIDE_LOG_FORMAT` | `text` or `json` | No |
| `PG_TIDE_LOG_LEVEL` | `error`, `warn`, `info`, `debug`, `trace` | No |
| `PG_TIDE_GROUP_ID` | Relay group ID for HA coordination | No |

### Docker Compose (complete development environment)

This sets up PostgreSQL with pg_tide, a NATS server, and the relay — everything you need for local development:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 5

  nats:
    image: nats:latest
    ports:
      - "4222:4222"
      - "8222:8222"

  pg-tide-relay:
    image: ghcr.io/trickle-labs/pg-tide:latest
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PG_TIDE_POSTGRES_URL: "postgres://postgres:postgres@postgres:5432/app"
      PG_TIDE_LOG_FORMAT: "json"
      PG_TIDE_LOG_LEVEL: "info"
    ports:
      - "9090:9090"
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:9090/health"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  pgdata:
```

### Building a custom image

If you need to bundle the extension with PostgreSQL:

```dockerfile
FROM postgres:18

# Copy compiled extension files
COPY pg_tide.so      /usr/lib/postgresql/18/lib/
COPY pg_tide.control /usr/share/postgresql/18/extension/
COPY sql/pg_tide--0.1.0.sql /usr/share/postgresql/18/extension/
```

---

## Kubernetes Deployment

For Kubernetes deployments, the relay runs as a standard Deployment with health checks, Prometheus metrics scraping, and optional horizontal scaling for HA.

### Basic deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-tide-relay
  labels:
    app: pg-tide-relay
spec:
  replicas: 2  # HA: advisory locks prevent duplicate processing
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
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: relay
          image: ghcr.io/trickle-labs/pg-tide:0.1.0
          env:
            - name: PG_TIDE_POSTGRES_URL
              valueFrom:
                secretKeyRef:
                  name: pg-tide-secrets
                  key: postgres-url
            - name: PG_TIDE_LOG_FORMAT
              value: "json"
            - name: PG_TIDE_GROUP_ID
              value: "production"
          ports:
            - containerPort: 9090
              name: metrics
          livenessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 3
            periodSeconds: 5
          resources:
            requests:
              cpu: 50m
              memory: 32Mi
            limits:
              cpu: 500m
              memory: 128Mi
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
```

### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pg-tide-secrets
type: Opaque
stringData:
  postgres-url: "postgres://pg_tide_relay:secret@pg-cluster-rw:5432/app?sslmode=require"
```

### Service (for metrics scraping)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: pg-tide-relay
  labels:
    app: pg-tide-relay
spec:
  selector:
    app: pg-tide-relay
  ports:
    - port: 9090
      targetPort: 9090
      name: metrics
```

### CloudNativePG integration

If you use [CloudNativePG](https://cloudnative-pg.io), deploy the relay as a sidecar alongside your PostgreSQL pods. The relay connects to `localhost:5432` via the CNPG-generated app secret:

```yaml
spec:
  sidecars:
    - name: pg-tide-relay
      image: ghcr.io/trickle-labs/pg-tide:0.1.0
      env:
        - name: PG_TIDE_POSTGRES_URL
          valueFrom:
            secretKeyRef:
              name: my-cluster-app
              key: uri
```

See [CloudNativePG Integration](../integration/cloudnativepg.md) for the complete setup.

### Helm chart

The project includes a Helm chart at `examples/helm/pg-tide/`:

```bash
helm install pg-tide-relay ./examples/helm/pg-tide \
  --set relay.postgresUrl="postgres://..." \
  --set relay.groupId="production" \
  --set relay.replicas=2
```

---

## High Availability

Running multiple relay instances with the same `relay_group_id` provides automatic failover:

```bash
# Instance A — acquires locks for pipelines 1, 3, 5
pg-tide run --relay-group-id production --postgres-url ...

# Instance B — acquires locks for pipelines 2, 4, 6
pg-tide run --relay-group-id production --postgres-url ...

# If Instance A crashes, Instance B acquires pipelines 1, 3, 5 within seconds
```

**How it works:**

1. Each relay instance attempts to acquire a PostgreSQL advisory lock for each pipeline
2. Only one instance can hold each lock — the lock owner processes that pipeline
3. If the owner crashes, its PostgreSQL session ends, locks are released
4. Other instances detect the released locks and acquire them on their next discovery cycle (every 30s by default, or immediately via LISTEN/NOTIFY)

**Important:** More replicas means faster failover, not more parallelism per pipeline. Each pipeline is always processed by exactly one relay instance.

---

## Resource Requirements

The relay is lightweight and predictable in its resource usage:

| Resource | Typical usage | Notes |
|----------|--------------|-------|
| **CPU** | ~50m per active pipeline | Scales with message volume and sink latency |
| **Memory** | 20-50 MB base + message buffer | Buffer grows with `batch_size` × average message size |
| **Network** | PostgreSQL connection + sink connections | 1 persistent PG connection + 1 LISTEN channel per instance |
| **Disk** | None | Completely stateless — all state in PostgreSQL |

For capacity planning: a relay instance handling 10 active pipelines at 1,000 messages/second total typically uses ~100m CPU and ~64 MB memory.

---

## Pre-Deployment Checklist

Before going live, verify each item:

- [ ] PostgreSQL 18 with `pg_tide` extension installed and verified
- [ ] Relay binary or Docker image available and version-pinned
- [ ] Pipeline configurations created in the database
- [ ] Consumer groups created for each forward pipeline
- [ ] Relay database user created with minimal privileges (see permissions above)
- [ ] TLS enabled for PostgreSQL connection (`sslmode=require`)
- [ ] Monitoring configured (Prometheus scrape target + alerting rules)
- [ ] Health check configured in load balancer / orchestrator
- [ ] At least 2 relay instances for HA (same `relay_group_id`)
- [ ] Log aggregation configured (structured JSON logs recommended)
- [ ] Backup strategy verified (standard PostgreSQL backups cover all pg_tide state)

---

## Zero-Downtime Upgrades

Upgrading the relay binary requires no downtime:

1. Deploy new relay instances alongside old ones (same `relay_group_id`)
2. New instances start and wait for advisory locks
3. Gracefully stop old instances (`SIGTERM`)
4. Old instances drain in-flight messages, commit final offsets, release locks
5. New instances acquire the released locks and resume processing
6. Committed events use at-least-once transport: loss is not silent, and
   duplicates remain possible until the destination deduplicates the stable ID

For Kubernetes rolling updates, this happens automatically:

```bash
kubectl set image deployment/pg-tide-relay relay=ghcr.io/trickle-labs/pg-tide:0.2.0
```

For the extension itself:

```sql
-- Check current version
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';

-- Upgrade (runs migration SQL automatically)
ALTER EXTENSION pg_tide UPDATE TO '0.2.0';
```

Always upgrade the extension before upgrading the relay binary.
