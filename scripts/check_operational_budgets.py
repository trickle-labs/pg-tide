#!/usr/bin/env python3
"""Validate and apply the v0.43 operational budget contract.

The checker deliberately has no benchmark runner knowledge.  A benchmark
produces JSON; this script validates its schema and compares numeric metrics
with the reviewed TOML budgets.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import tomllib


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUDGETS = ROOT / "benchmarks/operational/budgets.toml"
DEFAULT_BASELINE = ROOT / "benchmarks/operational/baseline.json"


class BudgetError(ValueError):
    """A malformed budget or incomparable benchmark result."""


def read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BudgetError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise BudgetError(f"{path}: root must be a table")
    return value


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BudgetError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise BudgetError(f"{path}: root must be an object")
    return value


def number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BudgetError(f"{label}: expected a number")
    result = float(value)
    if not math.isfinite(result):
        raise BudgetError(f"{label}: expected a finite number")
    return result


def metric_value(document: dict[str, Any], name: str) -> Any:
    metrics = document.get("metrics")
    if not isinstance(metrics, dict):
        raise BudgetError("benchmark JSON must contain a metrics object")
    if name in metrics:
        return metrics[name]
    current: Any = metrics
    for part in name.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def validate_config(config: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    if config.get("schema_version") != 1:
        raise BudgetError("budgets.toml: schema_version must be 1")
    required = config.get("required_metrics")
    budgets = config.get("metrics")
    if not isinstance(required, list) or not required or not all(
        isinstance(name, str) and name for name in required
    ):
        raise BudgetError("budgets.toml: required_metrics must be a non-empty string list")
    if not isinstance(budgets, dict):
        raise BudgetError("budgets.toml: metrics must be a table")
    if set(required) != set(budgets):
        missing = sorted(set(required) - set(budgets))
        extra = sorted(set(budgets) - set(required))
        details = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if extra:
            details.append(f"unexpected {', '.join(extra)}")
        raise BudgetError("budgets.toml: metric keys do not match required_metrics (" + "; ".join(details) + ")")

    baseline_schema = baseline.get("schema_version")
    if baseline_schema != config.get("benchmark_schema_version"):
        raise BudgetError(
            "baseline.json: schema_version does not match budgets.toml benchmark_schema_version"
        )
    baseline_metrics = baseline.get("metrics")
    if not isinstance(baseline_metrics, dict):
        raise BudgetError("baseline.json: metrics must be an object")

    for name in required:
        budget = budgets[name]
        if not isinstance(budget, dict):
            raise BudgetError(f"{name}: budget must be a table")
        direction = budget.get("direction")
        if direction not in {"lower_is_better", "higher_is_better"}:
            raise BudgetError(f"{name}: direction must be lower_is_better or higher_is_better")
        if not isinstance(budget.get("unit"), str) or not budget["unit"]:
            raise BudgetError(f"{name}: unit is required")
        bound_key = "max" if direction == "lower_is_better" else "min"
        if bound_key not in budget:
            raise BudgetError(f"{name}: {bound_key} bound is required")
        number(budget[bound_key], f"{name}.{bound_key}")
        if "max_regression_pct" in budget:
            regression = number(budget["max_regression_pct"], f"{name}.max_regression_pct")
            if regression < 0:
                raise BudgetError(f"{name}: max_regression_pct cannot be negative")
        if name not in baseline_metrics:
            raise BudgetError(f"baseline.json: missing metric {name}")
        if baseline_metrics[name] is not None:
            number(baseline_metrics[name], f"baseline.metrics.{name}")
    return required


def comparable(config: dict[str, Any], baseline: dict[str, Any], result: dict[str, Any]) -> None:
    expected = config.get("environment_schema_version")
    for label, document in (("baseline", baseline), ("result", result)):
        if document.get("schema_version") != config.get("benchmark_schema_version"):
            raise BudgetError(f"{label}: benchmark schema version is not comparable")
        environment = document.get("environment")
        if not isinstance(environment, dict):
            raise BudgetError(f"{label}: environment object is required")
        if expected and environment.get("schema_version") != expected:
            raise BudgetError(f"{label}: environment schema version is not comparable")
    baseline_env = baseline["environment"]
    result_env = result["environment"]
    for key in ("postgresql_major", "payload_bytes", "pipeline_count", "batch_size", "poll_interval_ms"):
        if key in baseline_env and baseline_env.get(key) != result_env.get(key):
            raise BudgetError(f"result: environment field {key} is not comparable with baseline")


def percent_change(value: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0 if value == 0 else math.inf
    return ((value - baseline) / abs(baseline)) * 100


def check_result(config: dict[str, Any], baseline: dict[str, Any], result: dict[str, Any]) -> int:
    comparable(config, baseline, result)
    failures = 0
    for name in config["required_metrics"]:
        budget = config["metrics"][name]
        measured = number(metric_value(result, name), f"result.metrics.{name}")
        direction = budget["direction"]
        bound = float(budget["max"] if direction == "lower_is_better" else budget["min"])
        passed = measured <= bound if direction == "lower_is_better" else measured >= bound
        baseline_value = baseline["metrics"].get(name)
        change = "n/a"
        if baseline_value is not None:
            change = f"{percent_change(measured, float(baseline_value)):+.2f}%"
            limit = float(budget.get("max_regression_pct", math.inf))
            if direction == "lower_is_better" and percent_change(measured, float(baseline_value)) > limit:
                passed = False
            if direction == "higher_is_better" and -percent_change(measured, float(baseline_value)) > limit:
                passed = False
        status = "PASS" if passed else "FAIL"
        print(
            f"{status} {name}: measured={measured:g} {budget['unit']} "
            f"budget={'≤' if direction == 'lower_is_better' else '≥'}{bound:g} "
            f"baseline={baseline_value if baseline_value is not None else 'pending'} change={change}"
        )
        failures += not passed
    return int(failures)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", nargs="?", type=Path, help="benchmark JSON result to check")
    parser.add_argument("--result", dest="result_option", type=Path, help="benchmark JSON result to check")
    parser.add_argument("--budgets", type=Path, default=DEFAULT_BUDGETS)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--check-config", action="store_true", help="validate TOML and baseline only")
    args = parser.parse_args()
    if args.result and args.result_option:
        parser.error("pass the result as a positional argument or with --result, not both")
    result_path = args.result_option or args.result
    try:
        config = read_toml(args.budgets)
        baseline = read_json(args.baseline)
        validate_config(config, baseline)
        if args.check_config:
            print("operational budget configuration is valid")
            return 0
        if result_path is None:
            parser.error("a benchmark result is required unless --check-config is used")
        return check_result(config, baseline, read_json(result_path))
    except BudgetError as exc:
        print(f"operational budget check failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
