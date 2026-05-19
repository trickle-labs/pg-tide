-- pg-tide × DuckLake getting-started initialisation script.
--
-- This script runs inside the PostgreSQL container on first boot.
-- It sets up the DuckLake catalog schema, a sample orders outbox, and
-- configures a DuckLake sink pipeline ready for the relay to pick up.

-- Enable the pg_tide extension.
CREATE EXTENSION IF NOT EXISTS pg_tide;

-- Create the orders outbox.
SELECT tide.outbox_create('orders', 'payload JSONB NOT NULL');

-- Configure a DuckLake sink pipeline.
SELECT tide.relay_set_outbox_v2(jsonb_build_object(
    'name',       'orders-ducklake',
    'outbox',     'orders',
    'sink_type',  'ducklake',
    'batch_size', 10,
    'enabled',    true,
    'config', jsonb_build_object(
        'data_path',          's3://pg-tide-lake/orders/',
        'namespace',          'pgtide',
        'table_template',     '{stream_table}',
        'catalog_schema',     'ducklake',
        'atomic_lake_writes', false,
        'inline_row_limit',   10,
        'partition',          'daily'
    )
));
