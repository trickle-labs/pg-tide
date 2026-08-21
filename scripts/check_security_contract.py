#!/usr/bin/env python3
"""Check that the v0.52 threat, evidence, and release contracts agree."""
from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
THREATS = ROOT / "docs/src/reference/threat-model.md"
EVIDENCE = ROOT / "docs/src/reference/security-evidence.md"
TESTS = ROOT / "tests/required-tests.toml"
RELEASE = ROOT / "release-evidence/v0.52.0-security.json"


def table_ids(path: Path, prefix: str, minimum_columns: int) -> set[str]:
    result = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| "):
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        if cells and re.fullmatch(rf"{prefix}[0-9]+", cells[0]):
            if len(cells) < minimum_columns:
                raise SystemExit(f"{path}: {cells[0]} has too few table columns")
            result.add(cells[0])
    return result


def main() -> int:
    expected = {f"T{number:02d}" for number in range(1, 18)}
    threats = table_ids(THREATS, "T", 10)
    evidence = table_ids(EVIDENCE, "T", 4)
    if threats != expected:
        raise SystemExit(f"threat model IDs do not equal T01-T17: {sorted(threats)}")
    if evidence != expected:
        raise SystemExit(f"security evidence IDs do not equal T01-T17: {sorted(evidence)}")

    manifest = tomllib.loads(TESTS.read_text(encoding="utf-8"))
    test_ids = {item["id"] for item in manifest.get("test", [])}
    release = json.loads(RELEASE.read_text(encoding="utf-8"))
    missing = set(release.get("required_results", [])) - test_ids
    if missing:
        raise SystemExit(f"release evidence references unknown required tests: {sorted(missing)}")
    if release.get("release") != "v0.52.0" or release.get("status") != "pending":
        raise SystemExit("v0.52 security evidence must remain pending for an implementation branch")
    graph_path = ROOT / release["dependency_graphs"]
    graphs = json.loads(graph_path.read_text(encoding="utf-8"))
    if graphs.get("release") != "v0.52.0" or graphs.get("status") != "pending":
        raise SystemExit("v0.52 dependency graph evidence must remain pending")
    if {item.get("profile") for item in graphs.get("profiles", [])} != {
        "core",
        "core-kafka",
        "pg18",
    }:
        raise SystemExit("dependency graph evidence must cover core, core-kafka, and pg18")
    print("security contract valid (T01-T17, required results, pending release evidence)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
