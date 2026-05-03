# Docker

The pg-tide relay is available as a Docker image from GitHub Container Registry.

---

## Quick Start

```bash
docker run -d \
  --name pg-tide-relay \
  -e PGTRICKLE_RELAY_POSTGRES_URL="postgres://user:pass@host.docker.internal:5432/mydb" \
  -p 9090:9090 \
  ghcr.io/trickle-labs/pg-tide:latest
```

---

## Image Details

| Property | Value |
|----------|-------|
| Base | Alpine 3.21 |
| Size | ~20 MB |
| User | `pgtide` (UID 1000) |
| Entrypoint | `pg-tide` |
| Exposed Port | 9090 (metrics + health) |

---

## Environment Variables

All configuration can be passed via environment variables:

```bash
docker run -d \
  -e PGTRICKLE_RELAY_POSTGRES_URL="postgres://..." \
  -e PGTRICKLE_RELAY_METRICS_ADDR="0.0.0.0:9090" \
  -e PGTRICKLE_RELAY_LOG_FORMAT="json" \
  -e PGTRICKLE_RELAY_LOG_LEVEL="info" \
  -e PGTRICKLE_RELAY_GROUP_ID="production" \
  ghcr.io/trickle-labs/pg-tide:0.1.0
```

---

## Docker Compose

```yaml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"

  pg-tide-relay:
    image: ghcr.io/trickle-labs/pg-tide:latest
    depends_on:
      - postgres
    environment:
      PGTRICKLE_RELAY_POSTGRES_URL: "postgres://postgres:postgres@postgres:5432/postgres"
      PGTRICKLE_RELAY_LOG_FORMAT: "json"
    ports:
      - "9090:9090"
```

---

## Building Locally

```bash
docker build -t pg-tide:local .
```

The multi-stage Dockerfile produces an optimized Alpine image with a statically-linked binary.

---

## Health Check

Configure Docker health checks:

```yaml
services:
  pg-tide-relay:
    image: ghcr.io/trickle-labs/pg-tide:latest
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:9090/health"]
      interval: 10s
      timeout: 5s
      retries: 3
```
