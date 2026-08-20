#!/usr/bin/env bash
set -uo pipefail

output_dir=/output
log_path="${output_dir}/extension.log"
list_path="${output_dir}/test-list.txt"
mkdir -p "$output_dir"

rustc --version > "${output_dir}/rustc-version.txt" 2>&1 || true
cargo --version > "${output_dir}/cargo-version.txt" 2>&1 || true
cargo pgrx --version > "${output_dir}/cargo-pgrx-version.txt" 2>&1 || true
pg_config --version > "${output_dir}/postgres-version.txt" 2>&1 || true
ulimit -c > "${output_dir}/core-limit.txt" 2>&1 || true

cargo pgrx test pg18 --package pg-tide-ext -- --list > "$list_path" 2>&1 || true

set +e
cargo pgrx test pg18 --package pg-tide-ext 2>&1 | tee "$log_path"
exit_code=${PIPESTATUS[0]}
set -e

index=0
while IFS= read -r -d '' log_file; do
    cp "$log_file" "${output_dir}/postgres-${index}.log"
    index=$((index + 1))
done < <(find /tmp /home/pgtide/.pgrx -type f -name '*.log' -print0 2>/dev/null || true)

python3 - "$exit_code" "$log_path" "$list_path" "${output_dir}/result.json" <<'PY'
import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

exit_code = int(sys.argv[1])
log_path = Path(sys.argv[2])
list_path = Path(sys.argv[3])
result_path = Path(sys.argv[4])
log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""


def command_output(command):
    try:
        return subprocess.run(command, check=False, capture_output=True, text=True).stdout.strip()
    except OSError as error:
        return f"unavailable: {error}"


failure = next(
    (line.strip() for line in log.splitlines() if re.search(r"FAILED|panicked at|^error:", line)),
    None,
)
if failure is None and exit_code:
    failure = f"extension test command exited with status {exit_code}"

def count(pattern):
    return sum(int(match) for match in re.findall(pattern, log))

result = {
    "schema_version": 1,
    "status": "passed" if exit_code == 0 else "failed",
    "exit_status": exit_code,
    "first_failure": failure,
    "passed_tests": count(r"(\d+) passed"),
    "failed_tests": count(r"(\d+) failed"),
    "skipped_tests": count(r"(\d+) skipped"),
    "environment": {
        "commit": os.environ.get("CLEANROOM_COMMIT"),
        "dirty": os.environ.get("CLEANROOM_DIRTY") == "true",
        "image_id": os.environ.get("CLEANROOM_IMAGE_ID"),
        "postgresql": command_output(["pg_config", "--version"]),
        "rust": command_output(["rustc", "--version"]),
        "cargo_pgrx": command_output(["cargo", "pgrx", "--version"]),
        "platform": platform.platform(),
        "architecture": platform.machine(),
    },
    "test_command": "cargo pgrx test pg18 --package pg-tide-ext",
    "artifacts": {
        "log": str(log_path.name),
        "test_list": str(list_path.name),
        "postgres_logs": sorted(path.name for path in result_path.parent.glob("postgres-*.log")),
        "core_limit": "core-limit.txt",
    },
    "workflow": {
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "job": os.environ.get("GITHUB_JOB", "extension-cleanroom"),
    },
    "completed_at": datetime.now(timezone.utc).isoformat(),
}
result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
PY

exit "$exit_code"