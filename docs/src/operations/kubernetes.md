# Kubernetes

Deploy the pg-tide relay in Kubernetes as a Deployment with health checks, metrics scraping, and optional HA.

---

## Basic Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-tide-relay
  labels:
    app: pg-tide-relay
spec:
  replicas: 2  # HA: advisory locks ensure no duplicate processing
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
            - name: PGTRICKLE_RELAY_POSTGRES_URL
              valueFrom:
                secretKeyRef:
                  name: pg-tide-secrets
                  key: postgres-url
            - name: PGTRICKLE_RELAY_LOG_FORMAT
              value: "json"
            - name: PGTRICKLE_RELAY_GROUP_ID
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
```

---

## Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pg-tide-secrets
type: Opaque
stringData:
  postgres-url: "postgres://relay:secret@pg-cluster-rw:5432/app"
```

---

## Service (for metrics scraping)

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

---

## CloudNativePG Integration

If you're using [CloudNativePG](https://cloudnative-pg.io), deploy the relay alongside your cluster:

```yaml
# Install pg_tide in your CNPG cluster via bootstrap SQL
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: app-cluster
spec:
  instances: 3
  postgresql:
    parameters:
      shared_preload_libraries: ""
  bootstrap:
    initdb:
      postInitSQL:
        - CREATE EXTENSION pg_tide
```

Then deploy the relay Deployment pointing at the CNPG read-write service (`app-cluster-rw`).

---

## Scaling

- **Vertical:** Increase CPU/memory limits for higher throughput per relay
- **Horizontal:** Increase replicas. Advisory locks distribute pipelines across instances automatically
- Each pipeline is handled by exactly one relay instance at a time
- More replicas = faster failover, not more parallelism per pipeline
