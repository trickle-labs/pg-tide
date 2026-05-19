#!/bin/sh
# Demo 3: "Streaming Sensor Dashboard" interactive demo
# Time: ~10 minutes
# Audience: IoT, data engineering, real-time analytics teams
#
# Shows: continuous ingest of sensor readings into DuckLake with
# data inlining (no Parquet files for small batches), then
# querying the live data from DuckDB.

set -eu
PSQL="psql -h postgres -U pgtide -d pgtide"

echo "===== DEMO: Streaming Sensor Dashboard ====="
echo ""

# Create a sensors outbox if it doesn't exist.
$PSQL -c "SELECT tide.outbox_create('sensors', 'payload JSONB NOT NULL');" 2>/dev/null || true

echo "Streaming 20 sensor readings (inline_row_limit=10, so first 10 go inline)..."
$PSQL -c "
DO \$\$
DECLARE
  i INT;
  sensors TEXT[] := ARRAY['temp', 'pressure', 'humidity', 'vibration'];
BEGIN
  FOR i IN 1..20 LOOP
    PERFORM tide.outbox_publish('sensors',
      jsonb_build_object(
        'sensor_id',   'sensor-' || (i % 4 + 1),
        'sensor_type', sensors[(i % 4) + 1],
        'value',       (random() * 100)::NUMERIC(6,2),
        'unit',        CASE (i % 4)
                         WHEN 0 THEN 'celsius'
                         WHEN 1 THEN 'hPa'
                         WHEN 2 THEN 'percent'
                         ELSE 'mm/s²'
                       END,
        'ts',          now() - ((20 - i) * INTERVAL '5 seconds')
      )
    );
  END LOOP;
END;
\$\$;
"
echo "Done. First 10 readings will be inlined (no Parquet file created)."
echo "Next 10 readings flush to a Parquet file when inline_row_limit is exceeded."
echo ""

echo "DuckLake offset map (shows snapshot-to-offset mapping):"
pg-tide ducklake offset-map \
  --pipeline sensors-ducklake \
  --postgres-url postgres://pgtide:pgtide@postgres:5432/pgtide 2>/dev/null || \
  echo "  (pipeline not yet configured — see init.sql to add sensors pipeline)"
echo ""

echo "In DuckDB, query the live sensor data:"
echo "  SELECT sensor_type, AVG(value), MAX(ts)"
echo "  FROM lake.pgtide.sensors"
echo "  GROUP BY sensor_type ORDER BY sensor_type;"
echo ""
echo "===== END DEMO ====="
