#!/usr/bin/env python3
"""Run small, locked, profile-aware production dependency checks."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCEPTIONS = ROOT / "supply-chain/advisory-exceptions.toml"


def run(command: list[str], *, offline: bool) -> str:
    if offline:
        command.append("--offline")
    try:
        return subprocess.check_output(command, cwd=ROOT, text=True, stderr=subprocess.STDOUT)
    except FileNotFoundError as exc:
        raise SystemExit(f"missing tool {command[0]!r}; install it or use --static") from exc
    except subprocess.CalledProcessError as exc:
        hint = "; retry without --offline if the local Cargo cache is incomplete" if offline else ""
        raise SystemExit(f"command failed: {' '.join(command)}{hint}\n{exc.output}") from exc


def profiles() -> list[tuple[str, list[str]]]:
    registry = tomllib.loads((ROOT / "connectors.toml").read_text(encoding="utf-8"))
    names = {profile for row in registry["connector"] for profile in row.get("profiles", [])}
    return [("core", ["core"]), ("core-kafka", ["core-kafka"]), ("pg18", [])] if names else []


def validate_exceptions() -> None:
    data = tomllib.loads(EXCEPTIONS.read_text(encoding="utf-8"))
    failures = []
    for item in data.get("exception", []):
        for field in ("advisory", "package", "owner", "reason", "opened", "expires", "removal_condition"):
            if not item.get(field):
                failures.append(f"{item.get('advisory', '<unknown>')}: missing {field}")
        if item.get("production_reachable", True):
            failures.append(f"{item.get('advisory', '<unknown>')}: production_reachable must be false")
        try:
            if dt.date.fromisoformat(item["expires"]) < dt.date.today():
                failures.append(f"{item['advisory']}: expired {item['expires']}")
        except (KeyError, ValueError):
            pass
    if failures:
        raise SystemExit("advisory policy failures:\n" + "\n".join(f"- {line}" for line in failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="use only the local Cargo cache")
    parser.add_argument("--static", action="store_true", help="validate policy without invoking Cargo")
    parser.add_argument("--report", type=Path, help="write a JSON report")
    args = parser.parse_args()
    validate_exceptions()
    report = {"schema_version": 1, "locked": True, "offline": args.offline, "profiles": []}
    for name, features in profiles():
        item = {"profile": name, "features": features}
        if args.static or name == "pg18":
            item["status"] = "static-only"
        else:
            tree = run(["cargo", "tree", "--package", "pg-tide-relay", "--no-default-features", "--features", features[0], "--locked", "--edges", "normal"], offline=args.offline)
            digest = hashlib.sha256(tree.encode()).hexdigest()
            forbidden = [name for name in ("kms-aws", "kms-gcp", "kms-vault") if name in tree]
            if forbidden:
                raise SystemExit(f"{name}: forbidden production dependencies: {', '.join(forbidden)}")
            item.update(status="checked", graph_sha256=digest, graph_lines=len(tree.splitlines()))
        report["profiles"].append(item)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
