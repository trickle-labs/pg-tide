#!/usr/bin/env python3
"""Build and run the authoritative PostgreSQL extension clean room."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests/extension-cleanroom"
DEFAULT_OUTPUT = ROOT / "target/extension-cleanroom"


def git_value(*args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def write_failure(output: Path, status: str, message: str, image: str) -> None:
    result = {
        "schema_version": 1,
        "status": status,
        "exit_status": 2,
        "first_failure": message,
        "environment": {
            "commit": git_value("rev-parse", "HEAD"),
            "dirty": bool(git_value("status", "--porcelain")),
            "image": image,
        },
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--image", help="override the generated Docker image tag")
    args = parser.parse_args()

    output = args.output if args.output.is_absolute() else ROOT / args.output
    output.mkdir(parents=True, exist_ok=True)
    output.chmod(0o777)
    image_key = hashlib.sha256(
        b"".join(path.read_bytes() for path in (HARNESS / "Dockerfile", ROOT / "Cargo.lock", ROOT / "rust-toolchain.toml"))
    ).hexdigest()[:16]
    image = args.image or os.environ.get("PG_TIDE_CLEANROOM_IMAGE", f"pg-tide-extension-cleanroom:{image_key}")

    docker = shutil.which("docker")
    if docker is None:
        message = "Docker is required for the authoritative extension clean room"
        print(f"clean room unavailable: {message}", file=sys.stderr)
        write_failure(output, "infrastructure_error", message, image)
        return 2

    build = subprocess.run(
        [
            docker,
            "build",
            "--pull",
            "--file",
            str(HARNESS / "Dockerfile"),
            "--tag",
            image,
            str(ROOT),
        ],
        check=False,
        cwd=ROOT,
    )
    if build.returncode:
        message = f"clean-room image build exited with status {build.returncode}"
        write_failure(output, "infrastructure_error", message, image)
        return build.returncode

    inspect = subprocess.run(
        [docker, "image", "inspect", "--format", "{{.Id}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    image_id = inspect.stdout.strip() if inspect.returncode == 0 else None
    env = os.environ.copy()
    commit = git_value("rev-parse", "HEAD")
    env.update(
        {
            "CLEANROOM_COMMIT": commit or "unknown",
            "CLEANROOM_DIRTY": "true" if git_value("status", "--porcelain") else "false",
            "CLEANROOM_IMAGE_ID": image_id or "unknown",
        }
    )
    run = subprocess.run(
        [
            docker,
            "run",
            "--rm",
            "--init",
            "--env",
            f"CLEANROOM_COMMIT={env['CLEANROOM_COMMIT']}",
            "--env",
            f"CLEANROOM_DIRTY={env['CLEANROOM_DIRTY']}",
            "--env",
            f"CLEANROOM_IMAGE_ID={env['CLEANROOM_IMAGE_ID']}",
            "--volume",
            f"{output}:/output",
            image,
        ],
        check=False,
        cwd=ROOT,
        env=env,
    )
    if not (output / "result.json").exists():
        write_failure(output, "infrastructure_error", "clean-room container produced no result.json", image)
    return run.returncode


if __name__ == "__main__":
    raise SystemExit(main())
