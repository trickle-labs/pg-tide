#!/bin/sh
# Demo 4: "Compliance Replay" enterprise demo
# Time: ~12 minutes
# Audience: Enterprise, compliance, financial services teams
#
# Shows: DuckDB time-travel replay from a pg-tide consumer offset,
# demonstrating how to reconstruct the exact state of the lake at any
# point in time for compliance and audit purposes.

set -eu
PSQL="psql -h postgres -U pgtide -d pgtide"

echo "===== DEMO: Compliance Replay ====="
echo ""

echo "Querying snapshot-to-offset map for the orders pipeline..."
$PSQL -c "
SELECT outbox_offset, snapshot_id, committed_at
FROM tide.ducklake_offset_map
WHERE pipeline_name = 'orders-ducklake'
ORDER BY outbox_offset ASC
LIMIT 10;
"
echo ""

echo "Getting DuckDB time-travel expression for offsets 1-50..."
$PSQL -c "SELECT tide.ducklake_replay_range('orders-ducklake', 1, 50);"
echo ""

echo "In DuckDB, replay events from offset range 1 to 50:"
echo "  -- Get the time-travel expression from pg-tide"
echo "  SELECT tide.ducklake_replay_range('orders-ducklake', 1, 50);"
echo "  -- Use it in DuckDB"
echo "  SELECT * FROM lake.pgtide.orders AT (VERSION => <from_snap>) .. AT (VERSION => <to_snap>);"
echo ""

echo "Consumer offset map (last 5 entries):"
pg-tide ducklake offset-map \
  --pipeline orders-ducklake \
  --limit 5 \
  --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide 2>/dev/null || \
  echo "  (no offset map entries yet — publish some events first)"
echo ""

echo "Key audit capabilities:"
echo "  • Every event is addressable by outbox offset AND DuckLake snapshot ID"
echo "  • Time-travel queries reproduce exact state at any past point"
echo "  • No message broker required — the data lake IS the event log"
echo ""
echo "===== END DEMO ====="
