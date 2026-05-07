# Integration: Microcks

[Microcks](https://microcks.io) is a CNCF incubating project for API mocking and
contract conformance testing. It ingests AsyncAPI, OpenAPI, gRPC, and GraphQL
specifications — including the document emitted by `pg-tide asyncapi export` — and
uses the examples within them to both mock event channels and verify that real
consumers and producers stay conformant to the contract.

This guide shows how to use `pg-tide asyncapi export` as a first-class integration
touchpoint: export your relay spec, import it into Microcks, develop against the
mocks, and gate your CI pipeline on contract conformance.

---

## Why This Matters

pg-tide publishes events from the database to any number of downstream consumers.
Those consumers need a stable, documented contract: "what topics exist, in what
format, with what shape of payload?" Today that contract lives implicitly inside
`pg-tide`'s relay configuration. The `asyncapi export` command makes it explicit —
and Microcks makes it enforceable.

```
PostgreSQL → pg-tide relay → [Kafka / NATS / SQS / …] → consumers
                    ↓
             asyncapi export
                    ↓
               Microcks ──► mock topics (development)
                        ──► conformance test (CI)
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| pg-tide CLI (`pg-tide`) | ≥ 0.14.0 |
| Docker / Docker Compose | any recent |
| Microcks (dev-mode image) | ≥ 1.11.0 |

---

## Step 1 — Export Your AsyncAPI Spec

Connect to a database that has relay pipelines configured and run:

```bash
pg-tide asyncapi export \
  --postgres-url "postgres://user:pass@localhost/mydb" \
  --format yaml \
  > relay-asyncapi.yaml
```

This produces an **AsyncAPI 3.0 YAML document** that enumerates every outbox
forward pipeline and every inbox reverse pipeline as named channels, operations,
and message schemas.

Example output excerpt:

```yaml
asyncapi: 3.0.0
info:
  title: pg-tide Relay AsyncAPI
  version: 0.16.0
  description: Auto-generated AsyncAPI 3.0 document from pg-tide relay catalog metadata.
channels:
  forward/orders:
    address: kafka/orders
    description: "Forward relay: outbox 'orders' → kafka"
    messages:
      ordersMessage:
        $ref: '#/components/messages/ordersMessage'
operations:
  sendOrders:
    action: send
    channel:
      $ref: '#/channels/forward~1orders'
    description: "Publish messages from outbox 'orders' to kafka"
components:
  messages:
    ordersMessage:
      name: ordersMessage
      contentType: application/json
      payload:
        type: object
        description: "pg_tide outbox message (wire_format: cloudevents)"
```

> **Enriching the spec:** The auto-generated payload schemas use `type: object`
> as a baseline. For full contract value, add JSON Schema definitions for your
> actual message payloads either by hand or by running `asyncapi export` against a
> database where schema evolution guardrails have already captured column types.

---

## Step 2 — Start Microcks Locally

The quickest path is Microcks' dev-mode Docker Compose, which bundles a Kafka
broker (Redpanda) alongside the Microcks server:

```bash
git clone https://github.com/microcks/microcks
cd microcks/install/docker-compose
docker compose -f docker-compose-devmode.yml up -d
```

Wait until all five containers are healthy, then open
[http://localhost:8080](http://localhost:8080).

---

## Step 3 — Import the Spec

### Via the Microcks UI

1. Go to **API | Services → Import**.
2. Upload `relay-asyncapi.yaml` as a *primary artifact*.
3. Microcks parses the channels and begins publishing mock messages to the
   embedded Kafka broker on the addresses defined in the spec.

### Via the Microcks REST API (CI-friendly)

```bash
curl -s -X POST http://localhost:8080/api/v1/artifact/upload \
  -H "Content-Type: multipart/form-data" \
  -F "file=@relay-asyncapi.yaml"
```

---

## Step 4 — Develop Against Mock Topics

Once imported, Microcks publishes mock events at regular intervals to Kafka topics
named after the channel addresses in your spec. Downstream consumer teams can
target the Microcks Kafka endpoint instead of a real pg-tide deployment:

```bash
# Confirm mock messages are flowing
kcat -b localhost:9092 -t kafka/orders -C -e

# Output:
# {"id":1,"type":"order.created","source":"/orders","data":{...}}
# {"id":2,"type":"order.updated","source":"/orders","data":{...}}
```

This means consumer teams can write and test their Kafka consumers in isolation —
no running PostgreSQL, no relay process, no seeded test data required.

---

## Step 5 — Run Conformance Tests

Once a real pg-tide relay is running (e.g., in a staging environment), use
Microcks to verify that the actual published events conform to the spec:

### Via the Microcks UI

1. Go to the imported **pg-tide Relay AsyncAPI** service.
2. Click **New Test**, set the endpoint to your staging Kafka broker
   (`kafka://staging-kafka:9092`), choose *AsyncAPI conformance* as the runner.
3. Microcks subscribes to the topic, collects messages, validates them against
   the schema and examples, and returns a conformance score.

### Via the REST API (CI step)

```bash
# Retrieve the service ID
SERVICE_ID=$(curl -s http://localhost:8080/api/v1/services \
  | jq -r '.[] | select(.name=="pg-tide Relay AsyncAPI") | .id')

# Launch the conformance test
TEST_ID=$(curl -s -X POST http://localhost:8080/api/v1/tests \
  -H "Content-Type: application/json" \
  -d '{
    "serviceId": "'"$SERVICE_ID"'",
    "testEndpoint": "kafka://staging-kafka:9092",
    "runnerType": "ASYNC_API_SCHEMA",
    "timeout": 15000
  }' | jq -r '.id')

echo "Test launched: $TEST_ID"

# Poll for result
sleep 20
curl -s http://localhost:8080/api/v1/tests/$TEST_ID \
  | jq '{success: .success, conformanceScore: .conformanceScore}'
```

A failing test means the relay is publishing events that violate the contract —
caught before production.

---

## Step 6 — Gate CI on the Contract

### GitHub Actions example

```yaml
name: Contract conformance

on:
  push:
    branches: [main]

jobs:
  contract-test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env: { POSTGRES_PASSWORD: test, POSTGRES_DB: ci }
        ports: ["5432:5432"]

    steps:
      - uses: actions/checkout@v4

      - name: Install pg_tide + seed pipelines
        run: |
          PGPASSWORD=test psql -h localhost -U postgres ci \
            -f sql/pg_tide--0.16.0.sql
          # ...register relay pipelines via tide.relay_*_upsert()...

      - name: Export AsyncAPI spec
        run: |
          pg-tide asyncapi export \
            --postgres-url "postgres://postgres:test@localhost/ci" \
            --format yaml > relay-asyncapi.yaml

      - name: Start Microcks
        run: |
          git clone --depth 1 https://github.com/microcks/microcks /tmp/microcks
          docker compose \
            -f /tmp/microcks/install/docker-compose/docker-compose-devmode.yml \
            up -d
          # Wait for readiness
          until curl -sf http://localhost:8080/api/v1/health; do sleep 2; done

      - name: Import spec into Microcks
        run: |
          curl -s -X POST http://localhost:8080/api/v1/artifact/upload \
            -F "file=@relay-asyncapi.yaml"

      - name: Verify mock topics are live
        run: |
          # At least one message must arrive within 10 s
          kcat -b localhost:9092 -t kafka/orders -C -c 1 -e -w 10

      - name: Run conformance test against relay
        run: |
          # Start relay against test DB pointing at Microcks Kafka
          pg-tide --postgres-url "postgres://postgres:test@localhost/ci" &
          sleep 5

          SERVICE_ID=$(curl -s http://localhost:8080/api/v1/services \
            | jq -r '.[] | select(.name=="pg-tide Relay AsyncAPI") | .id')

          RESULT=$(curl -s -X POST http://localhost:8080/api/v1/tests \
            -H "Content-Type: application/json" \
            -d "{\"serviceId\":\"$SERVICE_ID\",
                 \"testEndpoint\":\"kafka://localhost:9092\",
                 \"runnerType\":\"ASYNC_API_SCHEMA\",
                 \"timeout\":15000}")

          TEST_ID=$(echo $RESULT | jq -r '.id')
          sleep 20

          SUCCESS=$(curl -s http://localhost:8080/api/v1/tests/$TEST_ID \
            | jq -r '.success')

          echo "Contract test success: $SUCCESS"
          [ "$SUCCESS" = "true" ]
```

---

## Enriching the Generated Spec

The exported spec is intentionally minimal — `type: object` payload schemas are
safe placeholders. You can enrich it in two ways:

### Option A — Secondary artifact with example payloads

Create a `relay-examples.yaml` in the Microcks [API Examples Format](https://microcks.io/documentation/references/examples/)
and import it as a *secondary artifact* alongside `relay-asyncapi.yaml`. Microcks
merges the two: the spec provides structure, the examples provide realistic mock
data and dynamic templates.

```yaml
# relay-examples.yaml
apiVersion: mocks.microcks.io/v1alpha1
kind: APIExamples
metadata:
  name: pg-tide Relay AsyncAPI
  version: 0.16.0
operations:
  sendOrders:
    examples:
      order-created:
        value:
          id: "{{ uuid() }}"
          specversion: "1.0"
          type: "order.created"
          source: "/orders"
          time: "{{ now() }}"
          data:
            orderId: "{{ randomInt(1000,9999) }}"
            customerId: "{{ randomInt(100,999) }}"
            total: "{{ randomInt(10,500) }}.00"
```

### Option B — Emit richer schemas from the relay config

If your relay configuration stores JSON Schema definitions alongside the wire
format config, pg-tide can incorporate them into the exported spec. Open an issue
or contribute a PR to `pg-tide-relay/src/main.rs::run_asyncapi_export` to expose
this.

---

## Summary

| Step | What you get |
|---|---|
| `asyncapi export` | A machine-readable contract for every relay pipeline |
| Import into Microcks | Live mock Kafka/NATS/SQS topics for consumer development |
| Conformance test | Automated verification that the relay honours the contract |
| CI gate | Catch wire-format regressions before they reach production |

The `pg-tide asyncapi export` command is the bridge between pg-tide's relay
catalog and the broader API-contract ecosystem. Microcks is the natural home for
running and enforcing that contract.

## See Also

- [CLI Reference — asyncapi export](../relay-guide/cli-reference.md)
- [Wire Formats Overview](../wire-formats/overview.md)
- [GitHub Actions Integration](github-actions.md)
- [Microcks AsyncAPI tutorial](https://microcks.io/documentation/tutorials/first-asyncapi-mock/)
- [Microcks conformance testing](https://microcks.io/documentation/explanations/conformance-testing/)
