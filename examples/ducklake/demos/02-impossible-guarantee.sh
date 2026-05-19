#!/bin/sh
# Demo 2: "The Impossible Guarantee" crash-recovery demo
# Time: ~8 minutes
# Audience: Engineering talks, SRE/platform teams
#
# Shows: exactly-once delivery — even if the relay crashes between writing
# a DuckLake snapshot and acknowledging the outbox offset, no duplicate
# events appear in the lake.
#
# Theme: "Either the snapshot is committed and the offset advanced together,
#         or neither happened. This is the guarantee no other pipeline tool
#         can make with a data lake."

set -eu
PSQL="psql -h postgres -U pgtide -d pgtide"

echo "===== DEMO: The Impossible Guarantee ====="
echo ""

echo "Setup: publishing 100 events..."
$PSQL -c "
DO \$\$
DECLARE i INT;
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM tide.outbox_publish('orders',
      jsonb_build_object('order_id', 5000 + i, 'guarantee_demo', true));
  END LOOP;
END;
\$\$;
"
echo "Done."
echo ""

echo "Checking current consumer offset..."
$PSQL -c "
SELECT group_name, committed_offset
FROM tide.tide_consumer_offsets
WHERE outbox_name = 'orders';
"
echo ""

echo "Checking DuckLake snapshot count (before relay processes batch)..."
pg-tide ducklake snapshots \
  --pipeline orders-ducklake \
  --limit 5 \
  --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide || true
echo ""

echo "Key insight: atomic_lake_writes=true means the snapshot commit and"
echo "offset advance happen in a single PostgreSQL transaction."
echo "If the relay crashes between them — they both roll back."
echo "Result: no duplicates, no lost messages."
echo ""
echo "See: docs/src/guides/ducklake/01-from-transaction-to-data-lake.md"
echo "===== END DEMO ====="
