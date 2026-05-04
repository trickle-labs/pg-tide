# dbt Integration

[dbt (data build tool)](https://www.getdbt.com) transforms data inside
PostgreSQL using SQL `SELECT` statements. pg_tide can bridge the gap between
dbt-managed tables and external consumers: any dbt model that writes to a table
can trigger an outbox publish, and inbound events can feed into a staging table
that dbt reads.

## Pattern 1 — Publish dbt model output to the outbox

After a dbt model run completes, use a PostgreSQL trigger or an explicit
`CALL`/`SELECT` to publish new rows to the outbox:

### Trigger-based approach

```sql
-- Fired automatically whenever dbt inserts into the target table.
CREATE OR REPLACE FUNCTION public.on_customer_upsert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM tide.outbox_publish(
        'customers',
        row_to_json(NEW)::jsonb,
        '{"x-op": "upsert", "x-model": "dim_customers"}'::jsonb
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER publish_customer_events
    AFTER INSERT OR UPDATE ON public.dim_customers
    FOR EACH ROW EXECUTE FUNCTION public.on_customer_upsert();
```

### Post-hook approach (dbt project.yml)

```yaml
# dbt_project.yml
models:
  my_project:
    dim_customers:
      +post-hook: >
        INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers)
        SELECT
            'customers',
            row_to_json(t)::jsonb,
            '{}'::jsonb
        FROM {{ this }} t
        WHERE updated_at > '{{ run_started_at }}'
```

## Pattern 2 — Read inbox events in a dbt source

Configure the inbox table as a dbt source so that transformation models can
join against inbound events:

```yaml
# sources.yaml
sources:
  - name: tide
    schema: tide
    tables:
      - name: payments_inbox
        description: "Inbound payment events from Stripe via the pg_tide relay"
        columns:
          - name: event_id
            description: "Globally unique event identifier (dedup key)"
          - name: payload
            description: "Event payload as JSONB"
          - name: received_at
            description: "Timestamp when the event arrived in the inbox"
```

Then reference it in a dbt model:

```sql
-- models/stg_payments.sql
SELECT
    event_id,
    payload ->> 'payment_id'   AS payment_id,
    (payload ->> 'amount_cents')::int AS amount_cents,
    payload ->> 'currency'     AS currency,
    received_at
FROM {{ source('tide', 'payments_inbox') }}
WHERE processed_at IS NOT NULL
```

## Pattern 3 — Event-driven dbt runs

Use pg_notify to trigger a dbt run when new outbox messages arrive:

```sql
-- Notify an external listener whenever the outbox receives a new message.
CREATE OR REPLACE FUNCTION tide.notify_dbt_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('dbt_run_trigger', NEW.outbox_name);
    RETURN NEW;
END;
$$;

CREATE TRIGGER dbt_trigger
    AFTER INSERT ON tide.tide_outbox_messages
    FOR EACH ROW EXECUTE FUNCTION tide.notify_dbt_trigger();
```

A lightweight listener process (e.g. a Python script using `psycopg2`) can
then invoke `dbt run --select <model>` on demand.

## Best practices

- **Idempotency**: dbt runs must be idempotent. Use `ON CONFLICT DO NOTHING` or
  surrogate keys when writing to tables that publish to the outbox.
- **Dedup keys**: Set `event_id` in the payload to a deterministic key (e.g.
  `{{ dbt_utils.generate_surrogate_key(['order_id', 'updated_at']) }}`) so that
  the inbox deduplicates re-runs correctly.
- **Schema versioning**: Add a `_schema_version` field to every outbox payload
  so downstream consumers can handle schema evolution gracefully.
