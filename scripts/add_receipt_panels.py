#!/usr/bin/env python3
"""Add delivery receipt Grafana panels to relay-health.json."""
import json
import os

dashboard_path = os.path.join(os.path.dirname(__file__), '..', 'pg-tide', 'dashboards', 'relay-health.json')

with open(dashboard_path) as f:
    d = json.load(f)

panels = d.get('panels', [])

# Skip if already added
if any(p.get('title') == 'Delivery Receipt Rate' for p in panels):
    print("Panels already exist, skipping.")
    exit(0)

new_panels = [
    {
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 96},
        "id": 206,
        "title": "Delivery Receipts",
        "type": "row"
    },
    {
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"lineWidth": 2},
                "unit": "reqps"
            },
            "overrides": []
        },
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 97},
        "id": 207,
        "options": {
            "legend": {"calcs": ["mean", "max"], "displayMode": "table", "placement": "bottom"},
            "tooltip": {"mode": "multi"}
        },
        "targets": [
            {
                "expr": "rate(pg_tide_relay_receipts_written_total[1m])",
                "legendFormat": "{{pipeline}} ({{sink_type}})",
                "refId": "A"
            }
        ],
        "title": "Delivery Receipt Rate",
        "type": "timeseries"
    },
    {
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"lineWidth": 2},
                "unit": "s",
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 30},
                        {"color": "red", "value": 300}
                    ]
                }
            },
            "overrides": []
        },
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 97},
        "id": 208,
        "options": {
            "legend": {"calcs": ["mean", "max"], "displayMode": "table", "placement": "bottom"},
            "tooltip": {"mode": "multi"}
        },
        "targets": [
            {
                "expr": "sum by (pipeline) (increase(pg_tide_relay_receipts_written_total[5m]) == 0) * 300",
                "legendFormat": "{{pipeline}} receipt lag (s)",
                "refId": "A"
            }
        ],
        "title": "Receipt Lag (no new receipts window)",
        "type": "timeseries"
    }
]

d['panels'].extend(new_panels)

with open(dashboard_path, 'w') as f:
    json.dump(d, f, indent=2)

print(f"Added {len(new_panels)} panels. Total: {len(d['panels'])}")
