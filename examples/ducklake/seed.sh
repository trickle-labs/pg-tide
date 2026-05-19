#!/bin/sh
# pg-tide × DuckLake seeding script.
#
# Publishes 1 000 synthetic order events into the orders outbox and
# demonstrates querying the resulting DuckLake from DuckDB.
#
# Usage:
#   docker compose run seed

set -eu

PSQL="psql -h postgres -U pgtide -d pgtide"

echo "==> Publishing 1 000 synthetic order events..."
$PSQL -c "
DO \$\$
DECLARE
  i INT;
BEGIN
  FOR i IN 1..1000 LOOP
    PERFORM tide.outbox_publish(
      'orders',
      jsonb_build_object(
        'order_id',   i,
        'customer',   'customer-' || (i % 50 + 1),
        'product',    'product-' || (i % 20 + 1),
        'quantity',   (i % 5 + 1),
        'amount',     (random() * 500 + 10)::NUMERIC(10,2),
        'status',     CASE WHEN i % 3 = 0 THEN 'shipped'
                           WHEN i % 3 = 1 THEN 'pending'
                           ELSE 'delivered' END,
        'created_at', now() - ((1000 - i) * INTERVAL '1 minute')
      )
    );
  END LOOP;
END;
\$\$;
"

echo "==> Done! 1 000 orders published."
echo ""
echo "The pg-tide relay will now stream these to the DuckLake."
echo "Wait a few seconds, then run:"
echo ""
echo "  docker compose exec duckdb duckdb"
echo ""
echo "Inside DuckDB:"
echo "  INSTALL ducklake;"
echo "  LOAD ducklake;"
echo "  ATTACH 'ducklake:postgres:host=postgres user=pgtide password=pgtide dbname=pgtide' AS lake"
echo "       (DATA_PATH 's3://pg-tide-lake/orders/');"
echo "  SELECT * FROM lake.pgtide.orders LIMIT 10;"
echo "  -- Time-travel:"
echo "  SELECT * FROM lake.pgtide.orders AT (VERSION => 1) LIMIT 5;"
