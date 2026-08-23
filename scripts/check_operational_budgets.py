#!/usr/bin/env python3
"""Validate and apply the v1 operational budget contract.

The checker deliberately has no benchmark runner knowledge.  A benchmark
produces JSON; this script validates its schema and compares numeric metrics
with the reviewed TOML budgets.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any

import tomllib


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUDGETS = ROOT / "benchmarks/budgets-v1.toml"
DEFAULT_BASELINE = ROOT / "benchmarks/operational/baseline-v1.json"


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


def reject_pending(value: Any, label: str) -> None:
    if value is None or (isinstance(value, str) and value.lower() in {"pending", "tbd", "null"}):
        raise BudgetError(f"{label}: pending or null value")


def number(value: Any, label: str) -> float:
    reject_pending(value, label)
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
    if required is None or budgets is None:
        tables = next((config.get(key) for key in ("profiles", "profile_instances", "instances") if isinstance(config.get(key), dict)), None)
        if not tables:
            raise BudgetError("budgets.toml: required_metrics and metrics are required")
        for profile, table in tables.items():
            if not isinstance(table, dict) or not isinstance(table.get("required_metrics"), list) or not isinstance(table.get("metrics"), dict):
                raise BudgetError(f"{profile}: profile must define required_metrics and metrics")
        required, budgets = next(iter(tables.values())).get("required_metrics"), next(iter(tables.values())).get("metrics")
    if not isinstance(required, list) or not required or not all(
        isinstance(name, str) and name for name in required
    ):
        raise BudgetError("budgets.toml: required_metrics must be a non-empty string list")
    if not isinstance(budgets, dict):
        raise BudgetError("budgets.toml: metrics must be a table")
    profiles = config.get("required_profiles")
    if profiles is not None and (
        not isinstance(profiles, list)
        or not profiles
        or not all(isinstance(profile, str) and profile for profile in profiles)
    ):
        raise BudgetError("budgets.toml: required_profiles must be a non-empty string list")
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
    has_profile_baselines = any(isinstance(baseline.get(key), dict) for key in ("profiles", "profile_instances", "instances"))
    if not isinstance(baseline_metrics, dict) and not has_profile_baselines:
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
        if isinstance(baseline_metrics, dict) and name not in baseline_metrics:
            raise BudgetError(f"baseline.json: missing metric {name}")
        noise_floor = number(
            budget.get("noise_floor", budget.get("absolute_noise_floor")),
            f"{name}.noise_floor",
        )
        if noise_floor <= 0:
            raise BudgetError(f"{name}: noise_floor must be non-zero")
        for field in ("owner", "rationale"):
            if not isinstance(budget.get(field), str) or not budget[field].strip():
                raise BudgetError(f"{name}: {field} is required")
        if isinstance(baseline_metrics, dict):
            number(baseline_metrics.get(name), f"baseline.metrics.{name}")
    status = baseline.get("status")
    if isinstance(status, str) and status.lower() in {"pending", "pending_reference_run"}:
        raise BudgetError("baseline: pending status is not an accepted v1 baseline")
    return required


def profile_table(config: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return config
    for key in ("profiles", "profile_instances", "instances"):
        tables = config.get(key)
        if isinstance(tables, dict) and profile in tables and isinstance(tables[profile], dict):
            return tables[profile]
    return config


def profile_baseline(baseline: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return baseline
    for key in ("profiles", "profile_instances", "instances"):
        tables = baseline.get(key)
        if isinstance(tables, dict) and profile in tables and isinstance(tables[profile], dict):
            value = tables[profile]
            return value if "metrics" in value else {"metrics": value, "environment": baseline.get("environment")}
    return baseline


def environment_matches(expected: dict[str, Any], actual: dict[str, Any], identity: list[str] | None = None) -> str | None:
    fields = identity or [key for key in expected if key not in {"schema_version"}]
    for key in fields:
        if key in expected and expected.get(key) != actual.get(key):
            return f"environment field {key} differs"
    return None


def validate_run(result: dict[str, Any], label: str) -> None:
    status = result.get("status")
    reject_pending(status, f"{label}.status") if "status" in result else None
    if status in {"failed", "correctness_failed", "invalid"}:
        raise BudgetError(f"{label}: correctness failed")
    metadata = result.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    if (
        result.get("dirty") is True
        or result.get("dirty_state") in {"dirty", True}
        or result.get("git_dirty") is True
        or metadata.get("git_dirty") is True
    ):
        raise BudgetError(f"{label}: dirty checkout")
    correctness = result.get("correctness")
    if result.get("correctness_failed") is True or correctness in {"failed", "correctness_failed", False}:
        raise BudgetError(f"{label}: correctness failed")
    if isinstance(correctness, dict) and correctness.get("status") not in {None, "pass", "passed", "ok"}:
        raise BudgetError(f"{label}: correctness failed")
    for key in ("probe_error", "sample_error", "missing_probe"):
        if result.get(key):
            raise BudgetError(f"{label}: missing sample ({key})")


def baseline_metrics(document: dict[str, Any]) -> dict[str, Any]:
    metrics = document.get("metrics")
    if not isinstance(metrics, dict):
        raise BudgetError("baseline: metrics must be an object")
    return metrics


def metric_stat(value: Any, name: str, label: str) -> tuple[float, float]:
    if isinstance(value, dict):
        median_value = value.get("median", value.get("value"))
        mad_value = value.get("mad", value.get("median_absolute_deviation", 0))
    else:
        median_value, mad_value = value, 0
    return number(median_value, f"{label}.{name}"), number(mad_value, f"{label}.{name}.mad")


def mad(values: list[float]) -> float:
    centre = statistics.median(values)
    return statistics.median([abs(value - centre) for value in values])


def tier_policy(config: dict[str, Any], tier: str) -> tuple[int, float, float]:
    defaults = {"pr": (1, 25.0, 0.0), "scheduled": (3, 10.0, 0.0), "release": (3, 5.0, 0.0)}
    policy = config.get("tiers", {}).get(tier, {}) if isinstance(config.get("tiers"), dict) else {}
    default = defaults.get(tier)
    if default is None:
        raise BudgetError(f"unknown tier {tier}")
    minimum = int(policy.get("min_samples", policy.get("minimum_samples", default[0])))
    percentage = float(policy.get("regression_pct", policy.get("max_regression_pct", default[1])))
    absolute = float(policy.get("absolute_regression", policy.get("absolute_regression_allowance", default[2])))
    if minimum < 1 or percentage < 0 or absolute < 0:
        raise BudgetError(f"invalid {tier} tier policy")
    return minimum, percentage, absolute


def repeated_check(config: dict[str, Any], baseline: dict[str, Any], results: list[dict[str, Any]], tier: str, profile: str | None) -> tuple[int, dict[str, Any]]:
    minimum, tier_pct, absolute_allowance = tier_policy(config, tier)
    selected_config = profile_table(config, profile)
    selected_baseline = profile_baseline(baseline, profile)
    required = selected_config.get("required_metrics", config.get("required_metrics"))
    budgets = selected_config.get("metrics", config.get("metrics"))
    if not isinstance(required, list) or not isinstance(budgets, dict):
        raise BudgetError("profile must define required_metrics and metrics")
    classifications: dict[str, dict[str, Any]] = {}
    overall = "noise"
    try:
        if len(results) < minimum:
            raise BudgetError(f"need at least {minimum} result(s), got {len(results)}")
        for index, result in enumerate(results, 1):
            if profile and result.get("profile") not in {None, profile}:
                raise BudgetError(f"result {index}: profile does not match {profile}")
            validate_run(result, f"result {index}")
        expected_env = selected_baseline.get("environment") or baseline.get("environment")
        if not isinstance(expected_env, dict):
            raise BudgetError("baseline environment is required")
        identity = config.get("environment_identity", config.get("identity"))
        for index, result in enumerate(results, 1):
            environment = result.get("environment")
            if not isinstance(environment, dict):
                raise BudgetError(f"result {index}: environment object is required")
            mismatch = environment_matches(expected_env, environment, identity if isinstance(identity, list) else None)
            if mismatch:
                raise BudgetError(mismatch)
        base_metrics = baseline_metrics(selected_baseline)
        for name in sorted(required):
            if name not in budgets or not isinstance(budgets[name], dict):
                raise BudgetError(f"{name}: missing budget")
            values = [number(metric_value(result, name), f"result.metrics.{name}") for result in results]
            current = statistics.median(values)
            current_mad = mad(values)
            baseline_value, baseline_mad = metric_stat(base_metrics.get(name), name, "baseline.metrics")
            budget = budgets[name]
            direction = budget.get("direction")
            if direction not in {"lower_is_better", "higher_is_better"}:
                raise BudgetError(f"{name}: invalid direction")
            bound_key = "max" if direction == "lower_is_better" else "min"
            bound = number(budget.get(bound_key), f"{name}.{bound_key}")
            noise_floor = number(budget.get("noise_floor", budget.get("absolute_noise_floor", 0)), f"{name}.noise_floor")
            noise_pct = number(budget.get("noise_floor_pct", 0), f"{name}.noise_floor_pct")
            noise_band = max(noise_floor, 3 * baseline_mad, 3 * current_mad, abs(baseline_value) * noise_pct / 100)
            delta = current - baseline_value
            worsening = delta > 0 if direction == "lower_is_better" else delta < 0
            improving = delta < 0 if direction == "lower_is_better" else delta > 0
            relative_allowance = abs(baseline_value) * tier_pct / 100 if abs(baseline_value) > 1e-12 else absolute_allowance
            absolute_fail = current > bound if direction == "lower_is_better" else current < bound
            regression = worsening and abs(delta) > max(noise_band, relative_allowance)
            classification = "actionable_regression" if absolute_fail or regression else "improvement" if improving and abs(delta) > noise_band else "noise"
            classifications[name] = {"classification": classification, "median": current, "mad": current_mad, "baseline_median": baseline_value, "reason": "absolute budget exceeded" if absolute_fail else classification}
            if classification == "actionable_regression":
                overall = classification
            elif classification == "improvement" and overall == "noise":
                overall = classification
    except BudgetError as exc:
        classification = "missing_sample" if ("need at least" in str(exc) or "metrics" in str(exc) or "sample" in str(exc)) else "invalid_environment"
        overall = classification
        classifications = {"_overall": {"classification": classification, "reason": str(exc)}}
    report = {
        "profile": profile,
        "tier": tier,
        "classification": overall,
        "environment": {"classification": "comparable" if overall not in {"invalid_environment"} else "incomparable"},
        "metrics": classifications,
    }
    if isinstance(budgets, dict):
        report["budget"] = budgets
    return (0 if overall in {"noise", "improvement"} else 1), report


def comparable(config: dict[str, Any], baseline: dict[str, Any], result: dict[str, Any]) -> None:
    expected = config.get("reference_environment_schema_version", config.get("environment_schema_version"))
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
    validate_run(result, "result")
    failures = 0
    for name in config["required_metrics"]:
        budget = config["metrics"][name]
        measured = number(metric_value(result, name), f"result.metrics.{name}")
        direction = budget["direction"]
        bound = float(budget["max"] if direction == "lower_is_better" else budget["min"])
        passed = measured <= bound if direction == "lower_is_better" else measured >= bound
        baseline_value = baseline["metrics"].get(name)
        if baseline_value is None:
            raise BudgetError(f"baseline.metrics.{name}: pending or null value")
        baseline_value = number(baseline_value, f"baseline.metrics.{name}")
        change = "n/a"
        if baseline_value is not None:
            change = f"{percent_change(measured, baseline_value):+.2f}%"
            limit = float(budget.get("max_regression_pct", math.inf))
            if direction == "lower_is_better" and percent_change(measured, baseline_value) > limit:
                passed = False
            if direction == "higher_is_better" and -percent_change(measured, baseline_value) > limit:
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
    parser.add_argument("--result", dest="result_options", action="append", type=Path, help="benchmark JSON result to check (repeatable)")
    parser.add_argument("--budgets", type=Path, default=DEFAULT_BUDGETS)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--check-config", action="store_true", help="validate TOML and baseline only")
    parser.add_argument("--check-environment", type=Path, metavar="PATH", help="validate a captured environment")
    parser.add_argument("--tier", choices=("pr", "scheduled", "release"), default=None)
    parser.add_argument("--profile", help="profile or profile-instance key")
    parser.add_argument("--report", type=Path, help="write a deterministic comparison report")
    args = parser.parse_args()
    if args.result and args.result_options:
        parser.error("pass results positionally or with --result, not both")
    result_paths = ([args.result] if args.result else []) + (args.result_options or [])
    try:
        config = read_toml(args.budgets)
        baseline = read_json(args.baseline)
        validate_config(config, baseline)
        if args.check_config:
            print("operational budget configuration is valid")
            return 0
        if args.check_environment:
            captured = read_json(args.check_environment)
            environment = captured.get("environment", captured)
            expected = profile_baseline(baseline, args.profile).get("environment") or baseline.get("environment")
            if not isinstance(environment, dict) or not isinstance(expected, dict):
                raise BudgetError("environment object is required")
            identity = config.get("environment_identity", config.get("identity"))
            mismatch = environment_matches(expected, environment, identity if isinstance(identity, list) else None)
            if mismatch:
                raise BudgetError(mismatch)
            print("operational environment is comparable")
            return 0
        if not result_paths:
            parser.error("a benchmark result is required unless --check-config is used")
        if args.tier or args.profile or args.report or len(result_paths) > 1:
            tier = args.tier or "pr"
            results = [read_json(path) for path in result_paths]
            status, report = repeated_check(config, baseline, results, tier, args.profile)
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, sort_keys=True))
            return status
        return check_result(config, baseline, read_json(result_paths[0]))
    except BudgetError as exc:
        print(f"operational budget check failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
