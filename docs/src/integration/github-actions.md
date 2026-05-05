# Integration: GitHub Actions

This guide shows how to integrate pg_tide into your CI/CD pipeline with GitHub Actions — running tests against the extension, deploying relay instances, and managing database migrations.

## Testing pg_tide in CI

### Run Integration Tests

```yaml
name: Test pg_tide pipelines
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: test
          POSTGRES_DB: testdb
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4

      - name: Install pg_tide extension
        run: |
          # Install the extension into the test database
          PGPASSWORD=test psql -h localhost -U postgres -d testdb -f sql/pg_tide--0.11.0.sql

      - name: Create test outbox and inbox
        run: |
          PGPASSWORD=test psql -h localhost -U postgres -d testdb -c "
            SELECT tide.outbox_create('test_events');
            SELECT tide.inbox_create('test_inbox');
          "

      - name: Run application tests
        run: |
          DATABASE_URL="postgres://postgres:test@localhost/testdb" cargo test
        env:
          DATABASE_URL: postgres://postgres:test@localhost/testdb
```

### Validate Pipeline Configurations

```yaml
      - name: Validate pipeline configs
        run: |
          # Dry-run the relay to validate all pipeline configs parse correctly
          pg-tide \
            --postgres-url "postgres://postgres:test@localhost/testdb" \
            --validate-only
```

## Deploy Relay to Kubernetes

```yaml
name: Deploy pg-tide relay
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build and push relay image
        uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/${{ github.repository }}/pg-tide-relay:${{ github.sha }}

      - name: Deploy to Kubernetes
        uses: azure/k8s-deploy@v4
        with:
          manifests: k8s/relay-deployment.yaml
          images: ghcr.io/${{ github.repository }}/pg-tide-relay:${{ github.sha }}
```

## Database Migrations

### Apply Extension Upgrades

```yaml
name: Migrate pg_tide
on:
  workflow_dispatch:
    inputs:
      target_version:
        description: 'Target pg_tide version'
        required: true
        default: '0.11.0'

jobs:
  migrate:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Apply migration
        run: |
          psql "${DATABASE_URL}" -c "ALTER EXTENSION pg_tide UPDATE TO '${{ inputs.target_version }}';"
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
```

### Configure Pipelines on Deploy

```yaml
      - name: Apply pipeline configurations
        run: |
          for config_file in pipelines/*.sql; do
            echo "Applying $config_file"
            psql "${DATABASE_URL}" -f "$config_file"
          done
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
```

## Health Check After Deploy

```yaml
      - name: Wait for relay to be healthy
        run: |
          for i in $(seq 1 30); do
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://$RELAY_HOST:9090/health)
            if [ "$STATUS" = "200" ]; then
              echo "Relay is healthy"
              exit 0
            fi
            echo "Waiting... (attempt $i)"
            sleep 5
          done
          echo "Relay did not become healthy"
          exit 1
```

## Further Reading

- [Deployment Architectures](../operations/deployment-architectures.md) — Production topologies
- [Terraform Integration](terraform.md) — Infrastructure as code
