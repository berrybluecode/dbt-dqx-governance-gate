#!/usr/bin/env python3
"""Run dbt, select successful models from its artifacts, then enforce DQX."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT_DIR = ROOT / "example" / "dbt_project"


def warehouse_id_from_http_path(http_path: str | None) -> str | None:
    if not http_path:
        return None
    match = re.search(r"/warehouses/([^/]+)$", http_path.rstrip("/"))
    return match.group(1) if match else None


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Expected dbt artifact was not produced: {path}")
    return json.loads(path.read_text())


def successful_models(
    manifest: dict[str, Any],
    run_results: dict[str, Any],
) -> tuple[list[dict[str, str]], str, dict[str, int]]:
    """Return successful materialized models and dbt run metadata."""
    nodes = manifest.get("nodes", {})
    models: list[dict[str, str]] = []
    seen_model_ids: set[str] = set()
    status_counts: dict[str, int] = {}

    def add_model(unique_id: str) -> None:
        node = nodes.get(unique_id, {})
        relation_name = node.get("relation_name")
        if (
            node.get("resource_type") == "model"
            and relation_name
            and unique_id not in seen_model_ids
        ):
            models.append({"unique_id": unique_id, "relation_name": relation_name})
            seen_model_ids.add(unique_id)

    for result in run_results.get("results", []):
        unique_id = result["unique_id"]
        status = str(result.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
        node = nodes.get(unique_id, {})
        if node.get("resource_type") == "model" and status == "success":
            add_model(unique_id)
        elif node.get("resource_type") == "test" and status in {"pass", "warn"}:
            for dependency_id in node.get("depends_on", {}).get("nodes", []):
                add_model(dependency_id)

    invocation_id = run_results.get("metadata", {}).get("invocation_id", "unknown")
    return models, invocation_id, status_counts


def read_dbt_artifacts(
    project_dir: Path,
) -> tuple[list[dict[str, str]], str, dict[str, int]]:
    target = project_dir / "target"
    result = successful_models(
        load_json(target / "manifest.json"),
        load_json(target / "run_results.json"),
    )
    if not result[0]:
        raise RuntimeError("dbt completed but no successful materialized models were found")
    return result


def run_sql(
    statement: str,
    *,
    databricks: str,
    warehouse_id: str,
    env: dict[str, str],
) -> list[dict[str, Any]]:
    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
        }
    )
    completed = subprocess.run(
        [databricks, "api", "post", "/api/2.0/sql/statements", "--json", payload],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"Databricks SQL request failed: {detail}")
    try:
        body = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Databricks SQL returned a non-JSON response") from exc
    state = body.get("status", {}).get("state")
    if state != "SUCCEEDED":
        error = body.get("status", {}).get("error", {})
        raise RuntimeError(
            f"Databricks SQL statement did not succeed (state={state}): {error}"
        )
    columns = [column["name"] for column in body["manifest"]["schema"]["columns"]]
    rows = body.get("result", {}).get("data_array", []) or []
    return [dict(zip(columns, row)) for row in rows]


def governance_controls(
    metrics_table: str,
    unique_id: str,
    invocation_id: str,
    *,
    databricks: str,
    warehouse_id: str,
    env: dict[str, str],
) -> list[dict[str, Any]]:
    run_name = f"{unique_id}:{invocation_id}".replace("'", "''")
    rows = run_sql(
        f"SELECT metric_value FROM {metrics_table} "
        f"WHERE run_name = '{run_name}' AND metric_name = 'check_metrics' "
        f"ORDER BY run_time DESC LIMIT 1",
        databricks=databricks,
        warehouse_id=warehouse_id,
        env=env,
    )
    if not rows:
        return []
    try:
        return json.loads(rows[0]["metric_value"])
    except (json.JSONDecodeError, KeyError, TypeError):
        return []


def color(text: str, code: str, *, enabled: bool) -> str:
    return f"\033[{code}m{text}\033[0m" if enabled else text


def timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def progress_line(
    index: int,
    total: int,
    control_name: str,
    model_name: str,
    status: str,
    violation_count: int,
    *,
    colors: bool,
) -> str:
    """Format a DQX result like a dbt test result."""
    test_name = f"dqx_{control_name}_on_{model_name}"
    prefix = f"{index} of {total} {status:<4} {test_name} "
    suffix_text = status if status == "PASS" else f"{status} {violation_count}"
    status_color = {"PASS": "32", "WARN": "33", "FAIL": "31"}[status]
    suffix = f"[{color(suffix_text, status_color, enabled=colors)}]"
    dots = "." * max(3, 88 - len(prefix) - len(suffix_text) - 2)
    return f"{timestamp()}  {prefix}{dots} {suffix}"


def execute(command: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    print(f"\n$ {' '.join(command)}", flush=True)
    completed = subprocess.run(command, cwd=cwd, env=env, check=False)
    if completed.returncode:
        raise subprocess.CalledProcessError(completed.returncode, command)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run dbt and synchronously enforce central DQX controls."
    )
    parser.add_argument("--scenario", choices=("pass", "warn", "block"), default="pass")
    parser.add_argument("--select", default="dim_customer", help="dbt selector")
    parser.add_argument("--target", default="dev", help="Databricks bundle target")
    parser.add_argument("--project-dir", type=Path, default=DEFAULT_PROJECT_DIR)
    parser.add_argument("--catalog", default=os.getenv("DBT_DQX_CATALOG", "main"))
    parser.add_argument("--schema", default=os.getenv("DBT_DQX_SCHEMA", "dbt_dqx_demo"))
    parser.add_argument(
        "--warehouse-id",
        default=(
            os.getenv("DATABRICKS_WAREHOUSE_ID")
            or warehouse_id_from_http_path(os.getenv("DATABRICKS_HTTP_PATH"))
        ),
    )
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument(
        "--artifacts-only",
        action="store_true",
        help="Consume artifacts produced by an earlier dbt CI step.",
    )
    parser.add_argument(
        "--verbose-gate",
        action="store_true",
        help="Stream the raw Databricks job output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_dir = args.project_dir.resolve()
    dbt = shutil.which("dbt")
    databricks = shutil.which("databricks")
    if not dbt and not args.artifacts_only:
        print("dbt is not installed. Install requirements.txt first.")
        return 2
    if not databricks:
        print("Databricks CLI is not installed or not on PATH.")
        return 2
    if not args.warehouse_id:
        print("Set DATABRICKS_WAREHOUSE_ID or pass --warehouse-id.")
        return 2

    env = os.environ.copy()
    common = [
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]

    if not args.artifacts_only:
        try:
            if not args.skip_seed:
                execute([dbt, "seed", *common, "--full-refresh"], cwd=ROOT, env=env)
            execute(
                [
                    dbt,
                    "build",
                    *common,
                    "--select",
                    args.select,
                    "--vars",
                    json.dumps({"scenario": args.scenario}),
                ],
                cwd=ROOT,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            print(f"\ndbt failed with exit code {exc.returncode}; DQX was not started.")
            return exc.returncode

    models, invocation_id, statuses = read_dbt_artifacts(project_dir)
    print(f"\ndbt passed · invocation_id={invocation_id} · statuses={statuses}")

    payload = base64.b64encode(
        json.dumps(models, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    params = (
        f"models_b64={payload},invocation_id={invocation_id},"
        f"catalog={args.catalog},schema={args.schema}"
    )

    print("\nENTERPRISE DQX GATE")
    print(f"  submitting {len(models)} executed model(s)")
    gate_started = time.monotonic()
    completed = subprocess.run(
        [
            databricks,
            "bundle",
            "run",
            "dqx_governance_gate",
            "-t",
            args.target,
            "--params",
            params,
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=not args.verbose_gate,
        text=True,
    )
    elapsed = time.monotonic() - gate_started
    raw_output = "" if args.verbose_gate else (completed.stdout or "") + (completed.stderr or "")
    run_url = next(
        (line.strip() for line in raw_output.splitlines() if line.startswith("Run URL:")),
        None,
    )
    if run_url:
        print(f"  {run_url}")

    gate_runs_table = f"{args.catalog}.{args.schema}.governance_gate_runs"
    metrics_table = f"{args.catalog}.{args.schema}.dqx_metrics"
    escaped_invocation = invocation_id.replace("'", "''")
    try:
        rows = run_sql(
            f"SELECT dbt_unique_id, input_row_count FROM {gate_runs_table} "
            f"WHERE invocation_id = '{escaped_invocation}'",
            databricks=databricks,
            warehouse_id=args.warehouse_id,
            env=env,
        )
    except RuntimeError as exc:
        print(f"\nCould not read DQX results: {exc}")
        print("Verify the SQL warehouse ID, permissions, catalog, and schema.")
        return 2
    if not rows and completed.returncode == 0:
        print("\nNo centrally governed controls applied to the selected models.")
        print(
            f"{timestamp()}  Done. PASS=0 WARN=0 ERROR=0 "
            f"SKIP={len(models)} TOTAL=0"
        )
        return 0
    if not rows:
        print("\nThe gate did not record a result. Raw job output:")
        print(raw_output.rstrip())
        return completed.returncode or 1

    results: list[dict[str, Any]] = []
    evaluated_model_ids = {row["dbt_unique_id"] for row in rows}
    for row in rows:
        unique_id = row["dbt_unique_id"]
        model_name = unique_id.split(".")[-1]
        try:
            controls = governance_controls(
                metrics_table,
                unique_id,
                invocation_id,
                databricks=databricks,
                warehouse_id=args.warehouse_id,
                env=env,
            )
        except RuntimeError as exc:
            print(f"\nCould not read DQX control metrics: {exc}")
            print("Verify the SQL warehouse ID, permissions, catalog, and schema.")
            return 2
        for control in controls:
            errors = int(control.get("error_count", 0))
            warnings = int(control.get("warning_count", 0))
            status = "FAIL" if errors else ("WARN" if warnings else "PASS")
            results.append(
                {
                    "control_name": control["check_name"],
                    "model_name": model_name,
                    "status": status,
                    "violation_count": errors or warnings,
                    "input_row_count": int(row["input_row_count"]),
                }
            )

    if not results:
        print("The gate recorded a model result but no per-control metrics.")
        return completed.returncode or 1

    colors = "NO_COLOR" not in env and (
        sys.stdout.isatty() or env.get("FORCE_COLOR") == "1"
    )
    passed = sum(result["status"] == "PASS" for result in results)
    warned = sum(result["status"] == "WARN" for result in results)
    failed = sum(result["status"] == "FAIL" for result in results)
    skipped = len({model["unique_id"] for model in models} - evaluated_model_ids)

    print()
    for index, result in enumerate(results, start=1):
        print(
            progress_line(
                index,
                len(results),
                result["control_name"],
                result["model_name"],
                result["status"],
                result["violation_count"],
                colors=colors,
            )
        )

    for result in (item for item in results if item["status"] != "PASS"):
        heading = "Failure" if result["status"] == "FAIL" else "Warning"
        code = "31" if result["status"] == "FAIL" else "33"
        print()
        print(
            color(
                f"{heading} in governance control {result['control_name']} "
                f"on {result['model_name']}",
                code,
                enabled=colors,
            )
        )
        print(
            f"  Got {result['violation_count']} violating row(s) "
            f"of {result['input_row_count']}."
        )

    print(
        f"\n{timestamp()}  Finished running {len(results)} governance control(s) "
        f"in {elapsed:.2f}s."
    )
    print(
        f"{timestamp()}  Done. PASS={passed} WARN={warned} "
        f"ERROR={failed} SKIP={skipped} TOTAL={len(results)}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
