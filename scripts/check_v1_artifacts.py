#!/usr/bin/env python3
"""Dependency-free checks for v0.47 metrics, health, CLI, and envelope artifacts."""
import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"


def obj(path):
    with path.open(encoding="utf-8") as f:
        value = json.load(f)
    assert isinstance(value, dict), path
    return value


def main():
    source = (ROOT / "pg-tide-relay/src/metrics.rs").read_text(encoding="utf-8")
    constants = set(re.findall(r'pub const METRIC_[A-Z0-9_]+: &str\s*=\s*"([^"]+)"', source))
    with (SCHEMAS / "metrics-v1.tsv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    names = {row["name"] for row in rows}
    assert names == constants, (constants - names, names - constants)
    assert len(rows) == len(names)
    assert all(row["type"] in {"counter", "gauge", "histogram"} for row in rows)
    assert all(row["labels"] for row in rows)
    assert all(row["buckets"] for row in rows if row["type"] == "histogram")
    assert all(not row["buckets"] for row in rows if row["type"] != "histogram")

    health = obj(SCHEMAS / "health-v1.json")
    assert {route["path"] for route in health["routes"]} == {
        "/livez", "/readyz", "/health", "/healthz", "/metrics"
    }
    for path in ("/livez", "/readyz", "/health", "/healthz", "/metrics"):
        assert f'"{path}"' in source

    cli_schema = obj(SCHEMAS / "cli-json-v1.schema.json")
    assert cli_schema["$schema"].endswith("/draft/2020-12/schema")
    cli = obj(SCHEMAS / "cli-json-v1.fixtures.json")
    assert {envelope["command"] for envelope in cli["success"]} == {
        "doctor",
        "status",
        "config validate",
        "config export",
        "maintenance sweep",
    }
    for envelope in [*cli["success"], cli["failure"]]:
        assert envelope["schema_version"] == 1
        assert envelope["observed_at"].endswith("Z")
        assert envelope["ok"] == (envelope["data"] is not None)
        assert (envelope["error"] is None) == envelope["ok"]
    output = (ROOT / "pg-tide-relay/src/cmd/output.rs").read_text(encoding="utf-8")
    assert all(field in output for field in ("schema_version", "command", "ok", "observed_at", "data", "error"))

    pipeline = obj(SCHEMAS / "pipeline-config-v1.fixtures.json")
    assert len(pipeline["valid"]) == 5
    assert len(pipeline["invalid"]) == 5
    assert {
        fixture["config"]["sink_type"] for fixture in pipeline["valid"]
    } == {"inbox", "nats", "kafka", "webhook", "pg_outbox"}
    assert all(fixture["config"]["source_type"] == "outbox" for fixture in pipeline["valid"])

    event_schema = obj(SCHEMAS / "event-envelopes-v1.schema.json")
    assert set(event_schema["$defs"]) == {"native_relay_message", "native_wire_message", "cloud_event"}
    events = obj(SCHEMAS / "event-envelopes-v1.fixtures.json")["fixtures"]
    assert {fixture["format"] for fixture in events} == {
        "native_relay_message", "native_wire_message", "cloud_event"
    }
    for fixture in events:
        value = fixture["value"]
        if fixture["format"] == "native_relay_message":
            assert set(value) == {
                "dedup_key", "subject", "payload", "op", "is_full_refresh",
                "outbox_id", "refresh_id", "outbox_name", "headers", "created_at"
            }
        elif fixture["format"] == "native_wire_message":
            assert set(value) == {"outbox_id", "op", "stream_table", "payload"}
        else:
            assert value["specversion"] == "1.0"
            assert value["datacontenttype"] == "application/json"
    print("v0.47.0 artifacts: OK")


if __name__ == "__main__":
    main()
