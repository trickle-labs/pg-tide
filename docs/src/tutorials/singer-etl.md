# Tutorial: Singer/Meltano ETL Pipelines

This tutorial shows how to use pg_tide with Singer taps and targets to build ETL pipelines. You'll extract data from a SaaS API (HubSpot), load it into PostgreSQL, transform it, and export results to a data warehouse.

## What You'll Build

```
HubSpot API  →  tap-hubspot  →  pg_tide inbox  →  Transform  →  pg_tide outbox  →  target-snowflake
```

## Prerequisites

- PostgreSQL with pg_tide installed
- Python 3.8+ (for Singer taps/targets)
- A HubSpot account with API access (or substitute any Singer tap)

## Step 1: Install Singer Tap

```bash
pip install tap-hubspot
```

## Step 2: Configure Extraction into pg_tide

```sql
CREATE EXTENSION pg_tide;
SELECT tide.inbox_create('hubspot_contacts');

SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'hubspot-extraction',
    'inbox', 'hubspot_contacts',
    'source', 'singer',
    'config', '{
        "tap_command": "tap-hubspot",
        "tap_config": {
            "api_key": "${env:HUBSPOT_API_KEY}",
            "start_date": "2024-01-01T00:00:00Z"
        },
        "stream_filter": ["contacts", "companies", "deals"],
        "state_persistence": true
    }'::jsonb
  )
);
```

## Step 3: Start the Relay

```bash
export HUBSPOT_API_KEY="your-api-key"
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

The relay runs the tap, captures its output, and writes records into the inbox. STATE messages are persisted for incremental syncs.

## Step 4: Process Inbox Data

Query the inbox to see extracted records:

```sql
SELECT id, payload->>'email' as email, payload->>'company' as company
FROM tide.inbox_pending('hubspot_contacts')
WHERE payload->>'stream' = 'contacts'
LIMIT 10;
```

Process and transform:

```sql
-- Create a materialized view for analytics
CREATE MATERIALIZED VIEW contact_summary AS
SELECT 
    payload->>'company' as company,
    count(*) as contact_count,
    max((payload->>'last_activity_date')::date) as last_active
FROM tide.inbox_all('hubspot_contacts')
WHERE payload->>'stream' = 'contacts'
GROUP BY payload->>'company';

-- Mark processed
SELECT tide.inbox_mark_processed('hubspot_contacts', id)
FROM tide.inbox_pending('hubspot_contacts');
```

## Step 5: Export to Data Warehouse

Create an outbox for warehouse loading:

```sql
SELECT tide.outbox_create('warehouse_events');

-- Publish transformed data
SELECT tide.outbox_publish('warehouse_events', 'contacts', jsonb_build_object(
    'company', company,
    'contact_count', contact_count,
    'last_active', last_active
))
FROM contact_summary;
```

Configure Singer target export:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'to-snowflake',
    'outbox', 'warehouse_events',
    'sink_type', 'singer',
    'config', '{
        "target_command": "target-snowflake",
        "target_config": {
            "account": "${env:SNOWFLAKE_ACCOUNT}",
            "user": "${env:SNOWFLAKE_USER}",
            "password": "${env:SNOWFLAKE_PASSWORD}",
            "database": "ANALYTICS",
            "schema": "RAW"
        }
    }'::jsonb
  )
);
```

## Step 6: Schedule Incremental Syncs

Since pg_tide persists Singer STATE, each run only extracts new/changed records. Schedule periodic syncs with cron or a workflow orchestrator:

```bash
# Run every hour - only extracts changes since last run
0 * * * * pg-tide --postgres-url "..." --run-once
```

Or keep the relay running continuously — it will re-run the tap at configurable intervals.

## Available Taps (Examples)

| Tap | Data Source | Install |
|-----|------------|---------|
| tap-hubspot | HubSpot CRM | `pip install tap-hubspot` |
| tap-salesforce | Salesforce | `pip install tap-salesforce` |
| tap-stripe | Stripe payments | `pip install tap-stripe` |
| tap-github | GitHub repos | `pip install tap-github` |
| tap-postgres | PostgreSQL DB | `pip install tap-postgres` |
| tap-mysql | MySQL DB | `pip install tap-mysql` |
| tap-google-analytics | GA4 | `pip install tap-google-analytics` |
| tap-shopify | Shopify | `pip install tap-shopify` |

Browse 500+ taps at [hub.meltano.com](https://hub.meltano.com/extractors/).

## Further Reading

- [Sources: Singer](../sources/singer.md) — Singer source configuration
- [Sinks: Singer](../sinks/singer.md) — Singer target configuration
- [Singer Protocol](../features/singer-protocol.md) — Protocol details
