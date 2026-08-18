#!/usr/bin/env python3
"""Validate the v1 contract manifest and frozen connector surface."""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "schemas/v1-contract-manifest.toml"
SURFACES = {
    "sql.public.v1", "pipeline.config.v1", "metrics.core.v1", "health.http.v1",
    "cli.machine.v1", "event.native.v1", "event.cloudevents.v1", "connectors.supported.v1",
}
REQUIRED = {
    "id", "version", "normative_doc", "artifact", "source", "check", "owner",
    "codeowner", "classification", "evolution", "last_reviewed",
}
SUPPORTED_SINKS = {"postgresql-inbox", "nats-jetstream-sink", "kafka-sink", "webhook-sink"}


class ContractError(ValueError):
    pass


def load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ContractError(f"{path}: invalid TOML: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"{path}: root must be a table")
    return value


def file_ref(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise ContractError(f"{label}: expected a relative file path")
    path = (ROOT / value).resolve()
    if ROOT not in path.parents or not path.is_file():
        raise ContractError(f"{label}: missing file {value}")
    return path


def check_ref(command: str, surface: str) -> None:
    try:
        parts = shlex.split(command)
    except ValueError as exc:
        raise ContractError(f"{surface}: invalid check command: {exc}") from exc
    if not parts:
        raise ContractError(f"{surface}: empty check command")
    if parts[0] == "just":
        recipes = (ROOT / "justfile").read_text()
        if len(parts) != 2 or not re.search(rf"^{re.escape(parts[1])}:", recipes, re.MULTILINE):
            raise ContractError(f"{surface}: unknown just recipe")
    elif parts[0] == "python3":
        if len(parts) < 2:
            raise ContractError(f"{surface}: check has no script")
        file_ref(parts[1], f"{surface}.check")
    elif parts[0].startswith("scripts/"):
        file_ref(parts[0], f"{surface}.check")


def validate_manifest() -> list[dict[str, Any]]:
    data = load_toml(MANIFEST)
    if data.get("schema_version") != 1 or data.get("contract_version") != 1:
        raise ContractError(f"{MANIFEST}: schema and contract versions must be 1")
    rows = data.get("surface")
    if not isinstance(rows, list) or not rows:
        raise ContractError(f"{MANIFEST}: [[surface]] rows are required")
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ContractError(f"{MANIFEST}: surface rows must be tables")
        surface = row.get("id", "<unknown>")
        missing = sorted(REQUIRED - set(row))
        if missing:
            raise ContractError(f"{surface}: missing metadata {', '.join(missing)}")
        if not isinstance(surface, str) or surface in seen:
            raise ContractError(f"{surface}: duplicate or invalid id")
        seen.add(surface)
        if row["version"] != 1 or not re.fullmatch(r"[a-z0-9-]+\.[a-z0-9-]+\.v1", surface):
            raise ContractError(f"{surface}: invalid v1 id/version")
        if row["classification"] != "frozen" or not isinstance(row["evolution"], str) or not row["evolution"].strip():
            raise ContractError(f"{surface}: frozen classification and evolution rules are required")
        for field in ("normative_doc", "artifact", "source"):
            file_ref(row[field], f"{surface}.{field}")
        for field in ("check", "owner", "codeowner", "last_reviewed"):
            if not isinstance(row[field], str) or not row[field].strip():
                raise ContractError(f"{surface}: {field} is required")
        check_ref(row["check"], surface)
        if row["last_reviewed"] != "v0.47.0":
            raise ContractError(f"{surface}: last_reviewed must be v0.47.0")
    if seen != SURFACES:
        raise ContractError(f"{MANIFEST}: surface inventory mismatch")
    return rows


def validate_artifacts(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        path = file_ref(row["artifact"], f"{row['id']}.artifact")
        if row["id"] == "pipeline.config.v1":
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise ContractError(f"{row['id']}: invalid JSON artifact: {exc}") from exc
            if value.get("$id") != "https://pg-tide.dev/schemas/pipeline-config-v1.schema.json":
                raise ContractError(f"{row['id']}: unexpected schema id")
        elif not path.read_text().strip():
            raise ContractError(f"{row['id']}: empty artifact")


def validate_connectors() -> None:
    registry = load_toml(ROOT / "connectors.toml")
    rows = registry.get("connector")
    if not isinstance(rows, list):
        raise ContractError("connectors.toml: connector rows are required")
    found = {
        row.get("id") for row in rows
        if isinstance(row, dict) and row.get("kind") == "connector"
        and row.get("maturity") == "supported" and row.get("direction") == "sink"
    }
    if found != SUPPORTED_SINKS:
        raise ContractError(f"connectors.toml: supported sink rows are {sorted(SUPPORTED_SINKS)}, found {sorted(found)}")
    matrix = (ROOT / "docs/src/support/connector-compatibility.md").read_text()
    for connector in SUPPORTED_SINKS:
        if not re.search(rf"<a id=\"{re.escape(connector)}\"></a>{re.escape(connector)} \| sink \| supported \|", matrix):
            raise ContractError(f"connector matrix: missing {connector}")
    result = subprocess.run(
        [sys.executable, "scripts/generate_connector_surface.py", "--check"],
        cwd=ROOT, text=True, capture_output=True,
    )
    if result.returncode:
        raise ContractError((result.stdout + result.stderr).strip())


def main() -> int:
    try:
        rows = validate_manifest()
        validate_artifacts(rows)
        validate_connectors()
    except ContractError as exc:
        print(f"v1 contract check failed: {exc}", file=sys.stderr)
        return 1
    print(f"v1 contract manifest valid ({len(rows)} frozen surfaces)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
