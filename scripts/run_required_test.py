#!/usr/bin/env python3
"""Run one manifest command and retain a machine-readable execution record."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from check_required_tests import MANIFEST, InventoryError, check_manifest, load_manifest


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "tests/flake-registry.toml"


def git_value(*args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def active_flake(test_id: str) -> dict[str, object] | None:
    try:
        with REGISTRY.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return None
    for entry in document.get("flake", []):
        if entry.get("test_id") == test_id and entry.get("quarantine_status") == "active":
            return {
                "status": "active",
                "issue": entry.get("issue"),
                "owner": entry.get("owner"),
                "expires": entry.get("quarantine_expires"),
            }
    return None


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run_required_test.py TEST_ID [--output-dir PATH]", file=sys.stderr)
        return 2
    test_id = sys.argv[1]
    output_dir = ROOT / "target/required-tests"
    if "--output-dir" in sys.argv:
        index = sys.argv.index("--output-dir")
        try:
            output_dir = Path(sys.argv[index + 1])
        except IndexError:
            print("--output-dir requires a path", file=sys.stderr)
            return 2
        if not output_dir.is_absolute():
            output_dir = ROOT / output_dir

    try:
        rows = check_manifest(MANIFEST)
        document = load_manifest(MANIFEST)
    except InventoryError as error:
        print(f"required-test manifest invalid: {error}", file=sys.stderr)
        return 1
    row = next((entry for entry in rows if entry["id"] == test_id), None)
    if row is None:
        print(f"unknown required test: {test_id}", file=sys.stderr)
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / f"{test_id}.log"
    started = datetime.now(timezone.utc)
    command = row["command"]
    process = subprocess.Popen(
        ["bash", "-euo", "pipefail", "-c", command],
        cwd=ROOT,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    with log_path.open("w", encoding="utf-8") as log_file:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            log_file.write(line)
    return_code = process.wait()
    nested_result = ROOT / "target/extension-cleanroom/result.json"
    nested: dict[str, object] = {}
    if nested_result.is_file():
        try:
            nested = json.loads(nested_result.read_text())
        except json.JSONDecodeError:
            nested = {}
    log = log_path.read_text(encoding="utf-8", errors="replace")
    skipped = nested.get("skipped_tests", 0)
    if not isinstance(skipped, int):
        skipped = 0
    first_failure = nested.get("first_failure")
    if not isinstance(first_failure, str) or not first_failure:
        first_failure = next(
            (line.strip() for line in log.splitlines() if re.search(r"FAILED|panicked at|^error:", line)),
            None,
        )
    result = {
        "schema_version": 1,
        "id": test_id,
        "manifest_version": document["inventory_version"],
        "status": "passed" if return_code == 0 else nested.get("status", "failed"),
        "exit_status": return_code,
        "commit": git_value("rev-parse", "HEAD"),
        "command": command,
        "job": os.environ.get("GITHUB_JOB", row["ci_job"]),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "skipped_tests": skipped,
        "first_failure": first_failure,
        "retry_count": 0,
        "flake_disposition": active_flake(test_id),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "architecture": platform.machine(),
        },
        "artifacts": [str(log_path.relative_to(ROOT))],
    }
    if nested_result.is_file():
        result["artifacts"].append(str(nested_result.relative_to(ROOT)))
    (output_dir / f"{test_id}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())