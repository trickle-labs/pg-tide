#!/usr/bin/env python3
"""Validate the required-test inventory and its execution records."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests/required-tests.toml"
REQUIRED_FIELDS = {
    "id", "level", "triggers", "workflow", "ci_job", "command", "targets",
    "dependencies", "owner", "blocking", "max_retries", "allow_local_skip",
    "flake_policy", "evidence",
}
LEVELS = {"pr", "scheduled", "release"}
TRIGGERS = {"pull_request", "push", "schedule", "workflow_dispatch", "release"}


class InventoryError(ValueError):
    pass


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InventoryError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise InventoryError("manifest root must be a table")
    return value


def workflow_jobs(workflow: Path) -> set[str]:
    return set(re.findall(r"^  ([A-Za-z0-9_-]+):\s*$", workflow.read_text(), re.MULTILINE))


def just_recipes() -> set[str]:
    recipes = set()
    for line in (ROOT / "justfile").read_text().splitlines():
        match = re.match(r"^([A-Za-z0-9_-]+)(?:\s+[A-Z][A-Z0-9_]*)?:$", line)
        if match:
            recipes.add(match.group(1))
    return recipes


def check_manifest(path: Path = MANIFEST) -> list[dict[str, Any]]:
    document = load_manifest(path)
    if document.get("schema_version") != 1:
        raise InventoryError("schema_version must be 1")
    if not isinstance(document.get("inventory_version"), str) or not document["inventory_version"]:
        raise InventoryError("inventory_version is required")
    rows = document.get("test")
    if not isinstance(rows, list) or not rows:
        raise InventoryError("at least one [[test]] entry is required")

    recipes = just_recipes()
    seen: set[str] = set()
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise InventoryError(f"test entry {index} must be a table")
        missing = sorted(REQUIRED_FIELDS - set(row))
        if missing:
            raise InventoryError(f"test entry {index} missing: {', '.join(missing)}")
        test_id = row["id"]
        if not isinstance(test_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]+", test_id):
            raise InventoryError(f"test entry {index}: invalid id")
        if test_id in seen:
            raise InventoryError(f"duplicate test id: {test_id}")
        seen.add(test_id)
        if row["level"] not in LEVELS:
            raise InventoryError(f"{test_id}: level must be one of {sorted(LEVELS)}")
        if not isinstance(row["triggers"], list) or not row["triggers"] or not set(row["triggers"]) <= TRIGGERS:
            raise InventoryError(f"{test_id}: invalid triggers")
        if row["level"] == "pr" and "pull_request" not in row["triggers"]:
            raise InventoryError(f"{test_id}: PR tests must include pull_request")
        if row["level"] == "scheduled" and "schedule" not in row["triggers"]:
            raise InventoryError(f"{test_id}: scheduled tests must include schedule")
        if row["level"] == "release" and "release" not in row["triggers"]:
            raise InventoryError(f"{test_id}: release tests must include release")

        workflow = ROOT / row["workflow"]
        if not workflow.is_file():
            raise InventoryError(f"{test_id}: missing workflow {row['workflow']}")
        if row["ci_job"] not in workflow_jobs(workflow):
            raise InventoryError(f"{test_id}: job {row['ci_job']} is missing from {row['workflow']}")
        if not isinstance(row["owner"], str) or not row["owner"].startswith("@"):
            raise InventoryError(f"{test_id}: owner must identify a repository owner")
        if not isinstance(row["command"], str) or not row["command"].strip():
            raise InventoryError(f"{test_id}: command is required")
        for recipe in re.findall(r"(?:^|[;&|])\s*just\s+([a-z0-9_-]+)", row["command"]):
            if recipe not in recipes:
                raise InventoryError(f"{test_id}: unknown just recipe {recipe}")
        for reference in re.findall(r"(?:python3?|bash)\s+(scripts/[A-Za-z0-9_.-]+)", row["command"]):
            if not (ROOT / reference).is_file():
                raise InventoryError(f"{test_id}: missing command script {reference}")
        if not isinstance(row["targets"], list) or not row["targets"]:
            raise InventoryError(f"{test_id}: targets are required")
        for target in row["targets"]:
            if not isinstance(target, str) or not target:
                raise InventoryError(f"{test_id}: target names must be strings")
            target_path = ROOT / target
            if "/" in target or "." in target:
                if not target_path.exists():
                    raise InventoryError(f"{test_id}: missing target {target}")
            elif not target_path.exists():
                raise InventoryError(f"{test_id}: missing target {target}")
        for field in ("dependencies", "evidence"):
            if not isinstance(row[field], list) or not row[field]:
                raise InventoryError(f"{test_id}: {field} must be a non-empty list")
        if not isinstance(row["blocking"], bool) or not isinstance(row["allow_local_skip"], bool):
            raise InventoryError(f"{test_id}: blocking and allow_local_skip must be booleans")
        if not isinstance(row["max_retries"], int) or row["max_retries"] < 0:
            raise InventoryError(f"{test_id}: max_retries must be a non-negative integer")
        if row["max_retries"] and row["flake_policy"] == "never-quarantine":
            raise InventoryError(f"{test_id}: never-quarantine tests cannot allow retries")
        if row["flake_policy"] not in {"never-quarantine", "record-only"}:
            raise InventoryError(f"{test_id}: unsupported flake policy")
        for evidence in row["evidence"]:
            if not isinstance(evidence, str) or not evidence:
                raise InventoryError(f"{test_id}: evidence paths must be strings")

    return rows


def current_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def check_results(rows: list[dict[str, Any]], result_dir: Path, ids: list[str] | None) -> None:
    selected = {test_id for test_id in ids} if ids else {row["id"] for row in rows}
    by_id = {row["id"]: row for row in rows}
    unknown = sorted(selected - by_id.keys())
    if unknown:
        raise InventoryError(f"unknown result IDs: {', '.join(unknown)}")
    expected_commit = current_commit()
    document = load_manifest()
    for test_id in sorted(selected):
        row = by_id[test_id]
        path = result_dir / f"{test_id}.json"
        if not path.is_file():
            raise InventoryError(f"missing result for {test_id}: {path}")
        try:
            result = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise InventoryError(f"{test_id}: invalid result JSON: {error}") from error
        if result.get("schema_version") != 1:
            raise InventoryError(f"{test_id}: result schema_version must be 1")
        if result.get("id") != test_id:
            raise InventoryError(f"{test_id}: result id mismatch")
        if result.get("manifest_version") != document["inventory_version"]:
            raise InventoryError(f"{test_id}: manifest/result version mismatch")
        if expected_commit and result.get("commit") != expected_commit:
            raise InventoryError(f"{test_id}: result commit does not match HEAD")
        if result.get("job") != row["ci_job"]:
            raise InventoryError(f"{test_id}: result was not produced by {row['ci_job']}")
        if result.get("command") != row["command"]:
            raise InventoryError(f"{test_id}: result command does not match manifest")
        if result.get("status") != "passed":
            raise InventoryError(f"{test_id}: required result status is {result.get('status')!r}")
        skipped = result.get("skipped_tests")
        if not isinstance(skipped, int) or skipped < 0:
            raise InventoryError(f"{test_id}: skipped_tests must be a non-negative integer")
        if row["blocking"] and skipped:
            raise InventoryError(f"{test_id}: blocking test reports skipped tests")
        retries = result.get("retry_count", 0)
        if not isinstance(retries, int) or retries < 0 or retries > row["max_retries"]:
            raise InventoryError(f"{test_id}: unexpected retry count {retries!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--check-manifest", action="store_true")
    parser.add_argument("--check-results", type=Path)
    parser.add_argument("--ids", nargs="+")
    args = parser.parse_args()
    if bool(args.check_manifest) == bool(args.check_results):
        parser.error("choose exactly one of --check-manifest or --check-results")
    try:
        rows = check_manifest(args.manifest)
        if args.check_results:
            check_results(rows, args.check_results, args.ids)
            print(f"required-test results valid ({len(args.ids) if args.ids else len(rows)} entries)")
        else:
            print(f"required-test manifest valid ({len(rows)} entries)")
        return 0
    except (InventoryError, OSError, tomllib.TOMLDecodeError) as error:
        print(f"required-test check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())