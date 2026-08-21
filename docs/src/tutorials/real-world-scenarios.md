# Real-World Scenarios

This page demonstrates pg_tide in realistic business contexts. Each scenario shows a complete architecture with SQL setup, pipeline configuration, and operational guidance — giving you a blueprint for solving similar problems in your own systems.

---

## Scenario 1: E-Commerce Order Pipeline

**The situation:** You're building an e-commerce platform. When a customer places an order, multiple downstream systems need to know: the warehouse service ships the items, the analytics pipeline tracks revenue, the email service sends a confirmation, and the search index updates product availability.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL (pg_tide)                           │
│                                                                   │
│  orders table ─── outbox_publish() ──▶ "orders" outbox           │
│                                              │                    │
└──────────────────────────────────────────────┼────────────────────┘
                                               │
                              ┌─────────────────┼─────────────────┐
                              │                 │                  │
                              ▼                 ▼                  ▼
                     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
                     │ NATS        │  │ Kafka       │  │ Webhook     │
                     │ (warehouse) │  │ (analytics) │  │ (email svc) │
                     └─────────────┘  └─────────────┘  └─────────────┘
```

### Setup

```sql
-- Create the outbox
SELECT tide.outbox_create('orders', p_retention_hours := 72);

-- Three independent consumer groups — each tracks its own progress
SELECT tide.create_consumer_group('warehouse-relay', 'orders');
SELECT tide.create_consumer_group('analytics-relay', 'orders');
SELECT tide.create_consumer_group('email-relay', 'orders');

-- Three pipelines: same outbox, different destinations
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-warehouse',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats:4222',
      'subject', 'warehouse.orders.{event_type}'
    ),
    'batch_size', 50
  )
);

SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-analytics',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'kafka:9092',
      'topic', 'order-events',
      'compression', 'zstd'
    ),
    'batch_size', 500
  )
);

SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-email',
    'outbox', 'orders',
    'sink_type', 'webhook',
    'config', jsonb_build_object(
      'url', 'https://email-service.internal/hooks/orders',
      'timeout_ms', 5000,
      'headers', '{"Authorization": "Bearer ${ENV:EMAIL_SVC_TOKEN}"}'
    ),
    'batch_size', 10
  )
);
```

### Publishing events throughout the order lifecycle

```sql
-- When an order is placed
BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (1001, 'cust-42', 249.99, 'confirmed');

  SELECT tide.outbox_publish('orders',
    jsonb_build_object(
      'order_id', 1001,
      'customer_id', 'cust-42',
      'total', 249.99,
      'items', '[{"sku": "LAPTOP-15", "qty": 1}]'::jsonb,
      'shipping_address', '{"city": "Oslo", "country": "NO"}'::jsonb
    ),
    '{"event_type": "order.confirmed", "schema_version": "2.0"}'::jsonb
  );
COMMIT;

-- When the order ships
BEGIN;
  UPDATE orders SET status = 'shipped' WHERE id = 1001;

  SELECT tide.outbox_publish('orders',
    '{"order_id": 1001, "tracking_number": "NO-123-456", "carrier": "Posten"}'::jsonb,
    '{"event_type": "order.shipped"}'::jsonb
  );
COMMIT;
```

### Why this works well

- Each downstream system progresses independently — the email service can be slow without blocking the warehouse
- If the email service goes down, its messages accumulate in the outbox; the warehouse and analytics pipelines are unaffected
- The analytics pipeline uses large batches for Kafka efficiency
- The webhook pipeline uses small batches and a short timeout to detect email service issues quickly

---

## Scenario 2: Multi-Tenant SaaS Webhook Delivery

**The situation:** You're building a B2B SaaS platform where tenants configure webhook endpoints to receive events. Each tenant has different endpoint URLs, different reliability requirements, and different event volumes. You need retry logic, per-tenant isolation, and visibility into delivery status.

### Architecture

Each tenant gets their own outbox. This provides:
- Independent backpressure (one slow tenant doesn't block others)
- Per-tenant monitoring (consumer lag is per-outbox)
- Tenant-specific retention policies

### Setup

```sql
-- Tenant onboarding: create a dedicated outbox
CREATE OR REPLACE FUNCTION provision_tenant_outbox(tenant_id TEXT, webhook_url TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  -- Each tenant gets their own outbox
  PERFORM tide.outbox_create(
    'webhooks-' || tenant_id,
    p_retention_hours := 168  -- 7 days for webhook retry window
  );

  PERFORM tide.create_consumer_group(
    'webhook-delivery-' || tenant_id,
    'webhooks-' || tenant_id
  );

  -- Configure webhook delivery pipeline
  PERFORM tide.relay_set_outbox_v2(
    jsonb_build_object(
      'name', 'deliver-' || tenant_id,
      'outbox', 'webhooks-' || tenant_id,
      'sink_type', 'webhook',
      'config', jsonb_build_object(
        'url', webhook_url,
        'timeout_ms', 10000,
        'retry_codes', '[429, 500, 502, 503, 504]',
        'headers', jsonb_build_object(
          'X-Tenant-ID', tenant_id,
          'X-Webhook-Version', '2024-01-01'
        )
      ),
      'batch_size', 1  -- Deliver webhooks one at a time for ordering
    )
  );
END;
$$;

-- Provision a few tenants
SELECT provision_tenant_outbox('acme-corp', 'https://hooks.acme.com/events');
SELECT provision_tenant_outbox('globex-inc', 'https://api.globex.io/webhooks');
```

### Publishing tenant events

```sql
-- Publish an event for a specific tenant
SELECT tide.outbox_publish(
  'webhooks-acme-corp',
  jsonb_build_object(
    'event', 'invoice.paid',
    'data', jsonb_build_object(
      'invoice_id', 'inv-2025-001',
      'amount_cents', 49900,
      'currency', 'USD'
    ),
    'timestamp', now()
  ),
  '{"event_type": "invoice.paid"}'::jsonb
);
```

### Monitoring per-tenant delivery

```sql
-- Which tenants have delivery lag?
SELECT
  outbox_name,
  pending_count,
  oldest_at,
  now() - oldest_at AS oldest_age
FROM tide.outbox_pending
WHERE outbox_name LIKE 'webhooks-%'
  AND pending_count > 0
ORDER BY pending_count DESC;
```

---

## Scenario 3: Event-Driven Data Warehouse Loading

**The situation:** Your operational database (PostgreSQL) processes transactions throughout the day, and your analytics team needs those changes loaded into a data warehouse (Snowflake, BigQuery, or a PostgreSQL analytics replica). Instead of batch ETL jobs that run once an hour, you want near-real-time streaming of changes.

### Architecture

```
┌─────────────────┐         ┌──────────┐         ┌─────────────────┐
│  App Database   │         │ pg-tide  │         │  Data Warehouse │
│  (PostgreSQL)   │────────▶│  relay   │────────▶│  (staging area) │
│                 │  outbox  │         │  Kafka   │                 │
└─────────────────┘         └──────────┘         └────────┬────────┘
                                                           │
                                                    ┌──────▼──────┐
                                                    │  dbt / ETL  │
                                                    │  transforms │
                                                    └─────────────┘
```

### Setup

```sql
-- Outbox for dimension table changes
SELECT tide.outbox_create('dim-changes', p_retention_hours := 168);
SELECT tide.create_consumer_group('warehouse-loader', 'dim-changes');

-- Pipeline to Kafka (where Kafka Connect or a custom consumer loads into the warehouse)
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'dims-to-kafka',
    'outbox', 'dim-changes',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'kafka:9092',
      'topic', 'warehouse.dim-changes',
      'compression', 'zstd',
      'key', '{event_type}'  -- partition by entity type for parallel loading
    ),
    'batch_size', 500
  )
);
```

### Trigger-based publishing

For tables where every change should be captured:

```sql
-- Automatically publish changes to the customers dimension
CREATE OR REPLACE FUNCTION publish_customer_change()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  PERFORM tide.outbox_publish('dim-changes',
    jsonb_build_object(
      'entity', 'customer',
      'operation', TG_OP,
      'data', row_to_json(NEW)::jsonb,
      'changed_at', now()
    ),
    jsonb_build_object(
      'event_type', 'customer.' || lower(TG_OP),
      'table', TG_TABLE_NAME
    )
  );
  RETURN NEW;
END;
$$;

CREATE TRIGGER capture_customer_changes
  AFTER INSERT OR UPDATE ON customers
  FOR EACH ROW EXECUTE FUNCTION publish_customer_change();
```

### Why this works

- Changes stream in near-real-time (seconds, not hours)
- The outbox provides a reliable buffer if the warehouse is temporarily unavailable
- Kafka provides durable, replayable storage for the warehouse loader
- The trigger captures changes automatically without modifying application code
- You control exactly what gets published (unlike CDC which captures raw row changes)

---

## Scenario 4: Microservice Choreography (Saga Pattern)

**The situation:** You're processing an order that requires coordination across multiple services: reserve inventory, charge payment, schedule shipping. Rather than a central orchestrator, you want each service to react to events and publish its own events — choreography style.

### Architecture

Each service has its own database with pg_tide. Events flow through NATS:

```
Order Service                    Inventory Service              Payment Service
┌───────────┐                    ┌───────────┐                 ┌───────────┐
│ outbox:   │──▶ NATS ──▶       │ inbox:    │                 │ inbox:    │
│ "orders"  │  order.confirmed   │ "inv-req" │                 │ "pay-req" │
└───────────┘                    └─────┬─────┘                 └─────┬─────┘
                                       │                             │
┌───────────┐                    ┌─────▼─────┐                 ┌─────▼─────┐
│ inbox:    │◀── NATS ◀──       │ outbox:   │                 │ outbox:   │
│ "results" │  *.completed       │ "inv-out" │                 │ "pay-out" │
└───────────┘                    └───────────┘                 └───────────┘
```

### Order service setup

```sql
-- Outbox: publishes order events
SELECT tide.outbox_create('orders', p_retention_hours := 168);
SELECT tide.create_consumer_group('nats-fanout', 'orders');
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-fanout',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats:4222',
      'subject', 'orders.{event_type}'
    )
  )
);

-- Inbox: receives completion/failure events from other services
SELECT tide.inbox_create('order-results',
  p_max_retries := 10,
  p_processed_retention_hours := 168
);
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'results-from-services',
    'inbox', 'order-results',
    'source', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats:4222',
      'subject', 'orders.*.completed',
      'queue_group', 'order-svc'
    )
  )
);
```

### Inventory service setup

```sql
-- Inbox: receives inventory reservation requests
SELECT tide.inbox_create('inventory-requests', p_max_retries := 3);
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'inv-requests',
    'inbox', 'inventory-requests',
    'source', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats:4222',
      'subject', 'orders.order.confirmed',
      'queue_group', 'inventory-svc'
    )
  )
);

-- Outbox: publishes reservation results
SELECT tide.outbox_create('inventory-results', p_retention_hours := 72);
SELECT tide.create_consumer_group('inv-nats', 'inventory-results');
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'inv-results-out',
    'outbox', 'inventory-results',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'tls://nats:4222',
      'subject', 'orders.inventory.completed'
    )
  )
);
```

### The flow

1. Order service publishes `order.confirmed` → NATS
2. Inventory service receives it in its inbox, reserves stock, publishes `inventory.completed`
3. Payment service receives the original event, charges the card, publishes `payment.completed`
4. Order service receives both completion events in its inbox and updates order status

Each step is independently reliable:
- If the inventory service is down, messages accumulate in its inbox
- If a payment fails, it's retried up to `max_retries` times
- The order service's inbox deduplicates if any message is delivered twice
- Every service can be independently deployed, scaled, and debugged

---

## Scenario 5: Audit Trail and Compliance Logging

**The situation:** You need an immutable audit trail of every significant business action for regulatory compliance. The audit log must be tamper-resistant, queryable, and forwarded to long-term archival storage (S3, GCS, or a compliance platform).

### Architecture

```sql
-- Dedicated outbox for audit events (long retention, never disabled)
SELECT tide.outbox_create('audit-log',
  p_retention_hours := 8760,       -- 365 days local retention
  p_inline_threshold := 1000000    -- very high threshold (never pause auditing)
);

-- Forward to cloud storage via webhook (to an internal archival service)
SELECT tide.create_consumer_group('archive-relay', 'audit-log');
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'audit-to-archive',
    'outbox', 'audit-log',
    'sink_type', 'webhook',
    'config', jsonb_build_object(
      'url', 'https://compliance-archiver.internal/ingest',
      'timeout_ms', 30000,
      'headers', '{"Authorization": "Bearer ${ENV:ARCHIVE_TOKEN}"}'
    ),
    'batch_size', 100
  )
);

-- Also forward to Kafka for real-time compliance monitoring
SELECT tide.create_consumer_group('compliance-kafka', 'audit-log');
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'audit-to-kafka',
    'outbox', 'audit-log',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'kafka:9092',
      'topic', 'compliance.audit-events',
      'acks', 'all'
    ),
    'batch_size', 200
  )
);
```

### Publishing audit events

Create a helper function that standardizes the audit event format:

```sql
CREATE OR REPLACE FUNCTION audit_log(
  p_action TEXT,
  p_actor TEXT,
  p_resource_type TEXT,
  p_resource_id TEXT,
  p_details JSONB DEFAULT '{}'
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM tide.outbox_publish('audit-log',
    jsonb_build_object(
      'action', p_action,
      'actor', p_actor,
      'resource_type', p_resource_type,
      'resource_id', p_resource_id,
      'details', p_details,
      'timestamp', now(),
      'ip_address', inet_client_addr()::text
    ),
    jsonb_build_object(
      'event_type', 'audit.' || p_action,
      'compliance_class', 'SOC2'
    )
  );
END;
$$;
```

Use it throughout your application:

```sql
BEGIN;
  UPDATE users SET email = 'new@example.com' WHERE id = 42;
  SELECT audit_log('user.email_changed', 'admin-jane',
    'user', '42',
    '{"old_email": "old@example.com", "new_email": "new@example.com"}'::jsonb
  );
COMMIT;

BEGIN;
  DELETE FROM api_keys WHERE id = 7;
  SELECT audit_log('api_key.revoked', 'user-42',
    'api_key', '7',
    '{"reason": "compromised"}'::jsonb
  );
COMMIT;
```

### Why this works for compliance

- **Atomicity:** Audit events are committed with the action they describe. It's impossible to perform an action without creating an audit record.
- **Immutability:** The outbox table is append-only from the application's perspective. Once committed, an audit event cannot be altered.
- **Durability:** Messages are replicated to multiple destinations (archive + Kafka). Even if one destination fails, the other captures the event.
- **Queryability:** The audit log is a PostgreSQL table — you can run SQL queries for investigation.
- **Tamper detection:** Compare the local outbox with the archived copy to detect any discrepancies.

---

## Common Patterns Across Scenarios

### Pattern: Use headers for routing

All scenarios use the `headers` JSONB for metadata that controls routing, filtering, and versioning:

```sql
'{"event_type": "order.confirmed", "schema_version": "2.0", "tenant_id": "acme"}'
```

This keeps the payload clean (business data only) while providing rich metadata for infrastructure decisions.

### Pattern: One outbox per bounded context

Rather than one giant outbox for everything, create outboxes aligned with your domain boundaries:

- `orders` — order lifecycle events
- `inventory` — stock level changes
- `payments` — payment processing events
- `audit-log` — compliance events

This provides independent backpressure, monitoring, and retention per domain.

### Pattern: Multiple consumer groups for fan-out

A single outbox can serve many purposes. Each consumer group tracks its own position independently, so a slow consumer doesn't block fast ones.

### Pattern: Structured event schemas

Version your event payloads with a `schema_version` header. This allows consumers to handle schema evolution gracefully:

```sql
SELECT tide.outbox_publish('orders',
  '{"order_id": 1, "total": 99.99, "currency": "USD"}'::jsonb,
  '{"event_type": "order.confirmed", "schema_version": "2.0"}'::jsonb
);
```
