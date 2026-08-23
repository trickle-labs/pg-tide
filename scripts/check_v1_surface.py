#!/usr/bin/env python3
"""Check the v0.49.0 retained product boundary and its disposition."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DISPOSITION = ROOT / "schemas/v1-surface-disposition.json"
HUMAN = ROOT / "plans/V1_SURFACE_DISPOSITION.md"


class SurfaceError(ValueError):
    pass


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SurfaceError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise SurfaceError(f"{path}: root must be an object")
    return value


def item_rows(document: dict) -> list[dict]:
    rows: list[dict] = []
    items = document.get("items")
    if not isinstance(items, dict) or not items:
        raise SurfaceError("items must contain at least one category")
    for kind, values in items.items():
        if not isinstance(values, dict):
            raise SurfaceError(f"items.{kind}: expected an object")
        for classification in ("retain", "remove", "labs"):
            entries = values.get(classification, [])
            if not isinstance(entries, list):
                raise SurfaceError(f"items.{kind}.{classification}: expected a list")
            for entry in entries:
                if classification == "retain":
                    if not isinstance(entry, str) or not entry:
                        raise SurfaceError(f"items.{kind}.retain: item IDs must be strings")
                    row = {"id": f"{kind}:{entry}", "kind": kind, "classification": classification}
                else:
                    if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
                        raise SurfaceError(f"items.{kind}.{classification}: removal rows need an id")
                    if not entry.get("last_version") or not entry.get("replacement"):
                        raise SurfaceError(f"{entry.get('id', '<unknown>')}: removals need last_version and replacement")
                    if entry.get("state") not in {"legacy-present", "absent"}:
                        raise SurfaceError(f"{entry['id']}: invalid removal state")
                    row = {**entry, "id": f"{kind}:{entry['id']}", "kind": kind, "classification": classification}
                rows.append(row)
    ids = [row["id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise SurfaceError("item IDs must be unique")
    return rows


def current_inventory() -> dict[str, set[str]]:
    connectors = tomllib.loads((ROOT / "connectors.toml").read_text(encoding="utf-8"))
    connector_ids = {row["id"] for row in connectors.get("connector", [])}
    cargo = tomllib.loads((ROOT / "pg-tide-relay/Cargo.toml").read_text(encoding="utf-8"))
    features = set(cargo.get("features", {}))
    descriptors = (ROOT / "pg-tide-relay/src/descriptors.rs").read_text(encoding="utf-8")
    sources: set[str] = set()
    sinks: set[str] = set()
    for field, values in re.findall(r"(source_types|sink_types):\s*&\[([^]]*)\]", descriptors):
        names = set(re.findall(r'"([^"]+)"', values))
        (sources if field == "source_types" else sinks).update(names)
    profiles = {name for name in cargo.get("features", {}) if name in {"core", "core-kafka", "experimental-full"}}
    return {
        "connector": connector_ids,
        "feature": features,
        "runtime-source": sources,
        "runtime-sink": sinks,
        "profile": profiles,
    }


def check_inventory(rows: list[dict]) -> None:
    by_id = {row["id"]: row for row in rows}
    observed = current_inventory()
    for kind, values in observed.items():
        for value in sorted(values):
            item_id = f"{kind}:{value}"
            row = by_id.get(item_id)
            if row is None:
                raise SurfaceError(f"{item_id}: current surface is missing from the disposition")
            if row["classification"] in {"remove", "labs"} and row["state"] == "absent":
                raise SurfaceError(f"{item_id}: disposition says absent but the current surface still exposes it")


def check_boundary(document: dict, rows: list[dict]) -> None:
    boundary = document.get("current_boundary")
    if not isinstance(boundary, dict):
        raise SurfaceError("current_boundary is required")
    expected = {
        "source": ["outbox"],
        "production_destinations": ["inbox", "nats", "kafka", "webhook"],
        "diagnostic_destinations": ["stdout", "file"],
        "wire_formats": ["native", "cloudevents"],
        "release_profiles": ["core", "core-kafka"],
    }
    for field, value in expected.items():
        if boundary.get(field) != value:
            raise SurfaceError(f"current_boundary.{field} must be {value!r}")
    by_id = {row["id"]: row for row in rows}
    for kind, values in {
        "runtime-source": boundary["source"],
        "runtime-sink": boundary["production_destinations"] + boundary["diagnostic_destinations"],
        "profile": boundary["release_profiles"],
    }.items():
        for value in values:
            row = by_id.get(f"{kind}:{value}")
            if not row or row["classification"] != "retain":
                raise SurfaceError(f"{kind}:{value}: retained boundary item is not retained")


def active_doc_paths() -> set[Path]:
    paths = {ROOT / "README.md", ROOT / "SUPPORT.md", ROOT / "Dockerfile", ROOT / "pg-tide.example.toml"}
    summary = ROOT / "docs/src/SUMMARY.md"
    historical = False
    for line in summary.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            historical = line == "## Labs and Historical Material"
        if historical:
            continue
        links = re.findall(r"\]\(([^)#]+)", line)
        for link in links:
            path = (summary.parent / link).resolve()
            if (
                path.is_file()
                and "/archive/" not in str(path)
                and "/adr/" not in str(path)
                and path.name != "v1-migration-guide.md"
            ):
                paths.add(path)
    paths.update((ROOT / "helm/pg-tide").rglob("*"))
    paths.update((ROOT / ".github/workflows").glob("release.yml"))
    return {path for path in paths if path.is_file()}


def check_active_docs(document: dict) -> None:
    removed = []
    for kind, values in document["items"].items():
        for entry in values.get("remove", []) + values.get("labs", []):
            removed.append(entry["id"].split(":", 1)[-1])
    protected = {
        "outbox",
        "inbox",
        "nats",
        "kafka",
        "webhook",
        "stdout",
        "file",
        "native",
        "cloudevents",
        "core",
        "core-kafka",
        "template",
        "sweep",
    }
    patterns = [
        re.compile(rf"(?<![A-Za-z0-9_]){re.escape(value)}(?![A-Za-z0-9_])", re.IGNORECASE)
        for value in removed
        if value not in protected
    ]
    for path in active_doc_paths():
        text = path.read_text(encoding="utf-8")
        if path == ROOT / "README.md":
            text = re.sub(r"<!-- BEGIN GENERATED CONNECTORS -->.*?<!-- END GENERATED CONNECTORS -->", "", text, flags=re.S)
        for pattern in patterns:
            if pattern.search(text):
                raise SurfaceError(f"{path.relative_to(ROOT)}: active surface advertises removed item {pattern.pattern}")


def check_human_record() -> None:
    digest = hashlib.sha256(DISPOSITION.read_bytes()).hexdigest()
    match = re.search(r"disposition-sha256:\s*([0-9a-f]{64})", HUMAN.read_text(encoding="utf-8"))
    if not match or match.group(1) != digest:
        raise SurfaceError("human disposition digest does not match schemas/v1-surface-disposition.json")


def check_generated_connector_output() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/generate_connector_surface.py", "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise SurfaceError((result.stdout + result.stderr).strip())


def main() -> int:
    try:
        document = read_json(DISPOSITION)
        if document.get("schema_version") != 1 or document.get("release") != "v0.49.0":
            raise SurfaceError("disposition schema_version/release mismatch")
        rows = item_rows(document)
        check_inventory(rows)
        check_boundary(document, rows)
        check_active_docs(document)
        check_human_record()
        check_generated_connector_output()
    except (OSError, SurfaceError, tomllib.TOMLDecodeError) as error:
        print(f"v1 surface check failed: {error}", file=sys.stderr)
        return 1
    print(f"v1 surface valid ({len(rows)} disposition items; retained boundary is explicit)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
