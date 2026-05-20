#!/usr/bin/env python3
"""Add Inbox Fleet summary panel row to relay-health.json (v0.33.0)."""
import json
import os

DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "..", "pg-tide", "dashboards", "relay-health.json")

# Sentinel title to detect if panels were already added.
SENTINEL = "Inbox Fleet"

with open(DASHBOARD_PATH) as f:
    data = json.load(f)

panels = data.get("panels", [])

# Check if already added.
if any(p.get("title") == SENTINEL for p in panels):
    print("Panels already exist, skipping.")
    exit(0)

max_id = max(p.get("id", 0) for p in panels)

new_panels = [
    # Row header.
    {
        "id": max_id + 1,
        "type": "row",
        "title": "Inbox Fleet",
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 100},
        "panels": [],
    },
    # Inbox fleet summary table.
    # Queries tide.inbox_status(NULL) via a PostgreSQL datasource.
    # Refresh interval is set to 60s to avoid high-frequency N+1 load from Grafana.
    {
        "id": max_id + 2,
        "type": "table",
        "title": "Inbox Fleet",
        "description": (
            "Fleet-wide inbox status from tide.inbox_status(NULL). "
            "Refresh: 60s. "
            "Note: This query scales with inbox count (O(n)); "
            "do not set refresh below 30s."
        ),
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 101},
        "interval": "60s",
        "options": {
            "showHeader": True,
            "sortBy": [{"displayName": "pending", "desc": True}],
        },
        "fieldConfig": {
            "defaults": {"color": {"mode": "thresholds"}},
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "pending"},
                    "properties": [
                        {
                            "id": "thresholds",
                            "value": {
                                "mode": "absolute",
                                "steps": [
                                    {"color": "green", "value": None},
                                    {"color": "yellow", "value": 100},
                                    {"color": "red", "value": 1000},
                                ],
                            },
                        },
                        {"id": "custom.displayMode", "value": "color-background"},
                    ],
                }
            ],
        },
        "targets": [
            {
                "datasource": {"type": "postgres", "uid": "${DS_POSTGRES}"},
                "rawSql": (
                    "SELECT "
                    "  inbox_name, "
                    "  total_messages AS total, "
                    "  pending_messages AS pending, "
                    "  processed_messages AS processed, "
                    "  failed_messages AS failed, "
                    "  last_processed_at "
                    "FROM tide.inbox_status(NULL) "
                    "ORDER BY pending DESC"
                ),
                "format": "table",
                "refId": "A",
            }
        ],
        "transformations": [
            {"id": "organize", "options": {"excludeByName": {}, "renameByName": {}}}
        ],
    },
]

panels.extend(new_panels)
data["panels"] = panels

with open(DASHBOARD_PATH, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print(f"Added {len(new_panels)} Inbox Fleet panel(s) to {DASHBOARD_PATH}")
