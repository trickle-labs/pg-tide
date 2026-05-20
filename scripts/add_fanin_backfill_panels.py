#!/usr/bin/env python3
"""Add Fan-In and Backfill panels to the Grafana relay-health dashboard (v0.29.0)."""
import json
import sys

DASHBOARD_PATH = "pg-tide/dashboards/relay-health.json"

with open(DASHBOARD_PATH, "r") as f:
    d = json.load(f)

panels = d["panels"]
titles = [p.get("title", "") for p in panels]

if any("Fan-In" in t for t in titles):
    print("Fan-In panels already present, skipping.")
    sys.exit(0)

max_id = max(p.get("id", 0) for p in panels)
max_y = max(
    p.get("gridPos", {}).get("y", 0) + p.get("gridPos", {}).get("h", 0)
    for p in panels
)

new_panels = [
    {
        "id": max_id + 1,
        "type": "row",
        "title": "Fan-In Sources",
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": max_y},
    },
    {
        "id": max_id + 2,
        "type": "table",
        "title": "Fan-In Source Lag",
        "description": "Per-source consumer lag for all fan-in pipelines.",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": max_y + 1},
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
        "targets": [
            {
                "expr": "pg_tide_relay_fanin_source_lag",
                "legendFormat": "{{pipeline}} / {{outbox}}",
                "refId": "A",
            }
        ],
        "transformations": [{"id": "labelsToFields", "options": {}}],
    },
    {
        "id": max_id + 3,
        "type": "timeseries",
        "title": "Fan-In Messages Merged / sec",
        "description": "Rate of messages merged per source outbox in fan-in pipelines.",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": max_y + 1},
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "fieldConfig": {"defaults": {"unit": "reqps"}, "overrides": []},
        "options": {},
        "targets": [
            {
                "expr": "rate(pg_tide_relay_fanin_messages_merged_total[1m])",
                "legendFormat": "{{pipeline}}/{{outbox}}",
                "refId": "A",
            }
        ],
    },
    {
        "id": max_id + 4,
        "type": "row",
        "title": "Backfill Jobs",
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": max_y + 9},
    },
    {
        "id": max_id + 5,
        "type": "table",
        "title": "Backfill Jobs",
        "description": "Active backfill jobs with source outbox, rows processed, percent complete, and estimated completion.",
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": max_y + 10},
        "datasource": {"type": "postgres", "uid": "${pg_datasource}"},
        "fieldConfig": {
            "defaults": {},
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "pct_complete"},
                    "properties": [{"id": "unit", "value": "percent"}],
                }
            ],
        },
        "options": {},
        "targets": [
            {
                "rawSql": (
                    "SELECT job_name, outbox_name, status, rows_processed, rows_total, "
                    "CASE WHEN rows_total > 0 "
                    "THEN ROUND(rows_processed::numeric/rows_total*100,1) "
                    "ELSE 0 END AS pct_complete, created_at "
                    "FROM tide.backfill_jobs "
                    "WHERE status IN ('pending','running','paused') "
                    "ORDER BY created_at DESC LIMIT 20"
                ),
                "format": "table",
                "refId": "A",
            }
        ],
    },
]

d["panels"].extend(new_panels)

with open(DASHBOARD_PATH, "w") as f:
    json.dump(d, f, indent=2)

print(f"Added {len(new_panels)} panels. Total panels now: {len(d['panels'])}")
