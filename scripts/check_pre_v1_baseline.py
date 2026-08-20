#!/usr/bin/env python3
"""Validate the machine-readable pre-v1 baseline and its provenance."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "release-evidence/pre-v1-baseline/baseline.json"
COMMANDS = ROOT / "release-evidence/pre-v1-baseline/commands.json"
TREE = ROOT / "release-evidence/pre-v1-baseline/dependency-tree.txt"
SECTIONS = {"source_surface", "dependencies", "artifacts", "tests_and_ci", "documentation", "operational_evidence"}
LEAF_STATUSES = {"measured", "pending", "not_available", "not_applicable"}


class BaselineError(ValueError):
    pass


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BaselineError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise BaselineError(f"{path}: root must be an object")
    return value


def validate_measurements(section: str, value: dict[str, Any]) -> None:
    measurements = value.get("measurements")
    if not isinstance(measurements, dict) or not measurements:
        raise BaselineError(f"{section}: measurements are required")
    for name, measurement in measurements.items():
        if not isinstance(measurement, dict) or measurement.get("status") not in LEAF_STATUSES:
            raise BaselineError(f"{section}.{name}: invalid measurement status")
        status = measurement["status"]
        if status == "measured":
            if "value" not in measurement or not isinstance(measurement.get("method"), str) or not measurement["method"]:
                raise BaselineError(f"{section}.{name}: measured values need value and method")
        elif "reason" not in measurement or not isinstance(measurement["reason"], str) or not measurement["reason"]:
            raise BaselineError(f"{section}.{name}: pending values need a reason")
        if status != "measured" and "value" in measurement:
            raise BaselineError(f"{section}.{name}: unavailable values must not contain a guessed value")


def check() -> int:
    baseline = read_json(BASELINE)
    if baseline.get("schema_version") != 1 or baseline.get("release") != "v0.47.0":
        raise BaselineError("baseline schema_version/release mismatch")
    if baseline.get("status") != "complete-with-pending-fields":
        raise BaselineError("baseline must declare complete-with-pending-fields")
    if not re.fullmatch(r"[0-9a-f]{40}", baseline.get("captured_commit", "")):
        raise BaselineError("captured_commit must be a full commit SHA")
    if baseline.get("dirty") is not False:
        raise BaselineError("baseline must be captured from a clean checkout")
    if not isinstance(baseline.get("environment"), dict):
        raise BaselineError("environment is required")
    for section in SECTIONS:
        value = baseline.get(section)
        if not isinstance(value, dict):
            raise BaselineError(f"missing section {section}")
        validate_measurements(section, value)

    provenance = baseline.get("provenance")
    if not isinstance(provenance, dict) or provenance.get("dependency_tree") != str(TREE.relative_to(ROOT)):
        raise BaselineError("dependency tree provenance is missing")
    commands = read_json(COMMANDS)
    if commands.get("captured_commit") != baseline["captured_commit"] or commands.get("dirty") is not False:
        raise BaselineError("commands provenance does not match baseline")
    rows = commands.get("commands")
    if not isinstance(rows, list) or not rows or any(row.get("exit_status") != 0 for row in rows):
        raise BaselineError("all baseline capture commands must have exit_status 0")
    digest = hashlib.sha256(TREE.read_bytes()).hexdigest()
    if digest != baseline["provenance"].get("dependency_tree_sha256"):
        raise BaselineError("dependency tree digest does not match baseline")
    print("pre-v1 baseline valid: v0.47.0 clean-tag provenance and pending fields are explicit")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if not args.check:
        parser.error("--check is required")
    try:
        return check()
    except BaselineError as error:
        print(f"pre-v1 baseline invalid: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())