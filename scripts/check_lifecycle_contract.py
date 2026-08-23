#!/usr/bin/env python3
"""Check the current lifecycle policy against repository artifacts."""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "schemas/lifecycle-compatibility-v1.json"
DOC = ROOT / "docs/src/reference/version-compatibility.md"
INVENTORY = ROOT / "tests/required-tests.toml"
VERSION = re.compile(r"^\d+\.\d+\.\d+$")


class ContractError(ValueError):
    pass


def fail(message: str) -> None:
    raise ContractError(message)


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path.relative_to(ROOT)}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(ROOT)} must contain an object")
    return value


def check_migrations(policy: dict) -> None:
    migrations = policy.get("migrations")
    if not isinstance(migrations, list) or not migrations:
        fail("migrations must be a non-empty list")
    expected = policy["floor_version"]
    for row in migrations:
        if row.get("from") != expected:
            fail(f"migration chain is broken at {expected}")
        if not VERSION.fullmatch(row.get("to", "")):
            fail(f"invalid migration target: {row.get('to')!r}")
        expected = row["to"]
        path = ROOT / row.get("file", "")
        if not path.is_file() and row.get("status") != "pending":
            fail(f"missing forward migration: {path.relative_to(ROOT)}")
        if row.get("reversible"):
            reverse = row.get("reverse_file")
            if not reverse:
                fail(f"{row['from']} -> {row['to']} is reversible but has no reverse file")
            if not (ROOT / reverse).is_file():
                fail(f"missing reverse migration: {reverse}")
        elif row.get("reverse_file"):
            fail(f"irreversible migration declares a reverse file: {row['from']} -> {row['to']}")
    if expected != policy["target_version"]:
        fail(f"migration chain ends at {expected}, not {policy['target_version']}")


def check_matrix(policy: dict) -> None:
    rows = policy.get("matrix")
    if not isinstance(rows, list) or not rows:
        fail("matrix must be a non-empty list")
    try:
        inventory = tomllib.loads(INVENTORY.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot read required-test inventory: {error}")
    tests = {row.get("id"): row for row in inventory.get("test", [])}
    for row in rows:
        test_id = row.get("test")
        test = tests.get(test_id)
        if not test:
            fail(f"matrix row has no required test: {test_id}")
        if not isinstance(test.get("command"), str) or not test["command"].strip():
            fail(f"matrix test has no executable command: {test_id}")
    for required in policy.get("required_tests", []):
        test_id = required.get("id")
        test = tests.get(test_id)
        if not test:
            fail(f"required lifecycle test missing from inventory: {test_id}")
        expected_targets = set(required.get("targets", []))
        actual_targets = set(test.get("targets", []))
        missing = sorted(expected_targets - actual_targets)
        if missing:
            fail(f"{test_id} is missing policy targets: {', '.join(missing)}")


def matrix_markdown(policy: dict) -> str:
    lines = ["<!-- BEGIN LIFECYCLE MATRIX -->", "| Extension | Relay | Status | Required test |", "|---|---|---|---|"]
    lines.extend(f"| {row['extension']} | {row['relay']} | {row['status']} | `{row['test']}` |" for row in policy["matrix"])
    lines.append("<!-- END LIFECYCLE MATRIX -->")
    return "\n".join(lines)


def check_docs(policy: dict) -> None:
    try:
        text = DOC.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read documentation: {error}")
    match = re.search(r"<!-- BEGIN LIFECYCLE MATRIX -->.*?<!-- END LIFECYCLE MATRIX -->", text, re.S)
    if not match:
        fail("version-compatibility.md has no lifecycle matrix markers")
    if match.group(0).strip() != matrix_markdown(policy):
        fail("version-compatibility.md lifecycle matrix differs from policy")


def extract_version(path: Path, pattern: str) -> str | None:
    return (match.group(1) if (match := re.search(pattern, path.read_text(encoding="utf-8"))) else None)


def check_alignment(policy: dict) -> None:
    target = policy["target_version"]
    alignment = policy["version_alignment"]
    workspace = extract_version(ROOT / alignment["workspace"], r"(?ms)^\[workspace\.package\].*?^version\s*=\s*\"([^\"]+)\"")
    if workspace != target:
        fail(f"workspace version is {workspace!r}, expected {target}")
    for name in alignment["controls"]:
        if extract_version(ROOT / name, r"(?m)^default_version\s*=\s*'([^']+)'$") != target:
            fail(f"{name} does not declare {target}")
    chart = ROOT / alignment["chart"]
    if extract_version(chart, r"(?m)^version:\s*([^\s]+)$") != target or extract_version(chart, r'(?m)^appVersion:\s*"([^"]+)"$') != target:
        fail(f"{alignment['chart']} does not declare {target}")
    for name in alignment["examples"]:
        path = ROOT / name
        if not path.is_file() or not any(
            marker in path.read_text(encoding="utf-8") for marker in (f":{target}", f"-{target}")
        ):
            fail(f"{name} does not reference relay {target}")
    evidence = ROOT / alignment["evidence"]
    if not evidence.is_file():
        fail(f"missing target evidence: {alignment['evidence']}")
    if read_json(evidence).get("release") != f"v{target}":
        fail(f"{alignment['evidence']} does not target v{target}")


def check_policy(policy: dict) -> None:
    if policy.get("schema_version") != 1:
        fail("schema_version must be 1")
    if policy.get("target_version") != "0.54.0" or policy.get("floor_version") != "0.47.0":
        fail("policy must cover v0.47.0 through v0.54.0")
    if policy.get("compatibility_error_code") != "PGTIDE_EXTENSION_VERSION_INCOMPATIBLE":
        fail("unexpected compatibility error code")
    check_migrations(policy)
    check_matrix(policy)
    check_docs(policy)
    check_alignment(policy)


def main() -> int:
    try:
        check_policy(read_json(POLICY))
    except (ContractError, OSError, KeyError, TypeError) as error:
        print(f"lifecycle contract check failed: {error}", file=sys.stderr)
        return 1
    print("lifecycle contract valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
