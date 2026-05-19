#!/bin/sh
# Demo 1: "Zero to Data Lake" lightning demo
# Time: ~5 minutes
# Audience: Conference talks, meetups
#
# Shows: docker compose up, publish events, query from DuckDB.
# Theme: "From a PostgreSQL transaction to a queryable data lake in 5 minutes."

set -eu
PSQL="psql -h postgres -U pgtide -d pgtide"

echo "===== DEMO: Zero to Data Lake ====="
echo ""
echo "Step 1: The pg_tide extension is already running in PostgreSQL."
echo "        The relay is streaming events to a DuckLake."
echo ""

# Publish a small batch of events to show immediate availability.
echo "Step 2: Publishing 5 order events..."
$PSQL -c "
DO \$\$
DECLARE i INT;
BEGIN
  FOR i IN 1..5 LOOP
    PERFORM tide.outbox_publish('orders',
      jsonb_build_object('order_id', 9000 + i, 'demo', true, 'item', 'widget-' || i));
  END LOOP;
END;
\$\$;
"
echo "        Done! Events written atomically inside a PostgreSQL transaction."
echo ""

echo "Step 3: Checking pg-tide relay status..."
pg-tide status --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide || true
echo ""

echo "Step 4: Inspecting DuckLake snapshots..."
pg-tide ducklake snapshots \
  --pipeline orders-ducklake \
  --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide || true
echo ""

echo "Step 5: In DuckDB, run:"
echo ""
echo "  INSTALL ducklake; LOAD ducklake;"
echo "  ATTACH 'ducklake:postgres:host=postgres user=pgtide password=pgtide dbname=pgtide'"
echo "       AS lake (DATA_PATH 's3://pg-tide-lake/orders/');"
echo "  SELECT count(*), max(order_id) FROM lake.pgtide.orders;"
echo ""
echo "===== END DEMO ====="
