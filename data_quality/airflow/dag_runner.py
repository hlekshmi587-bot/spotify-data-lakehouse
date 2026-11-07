"""Standalone local DAG runner for Data Quality Airflow pipeline.

Executes DAG tasks in topological order.
Verifies failure propagation: if an upstream task fails, downstream tasks are blocked.
Saves metrics strictly to data_quality/reports/pipeline_metrics.json.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


def get_project_root() -> Path:
    """Resolve the absolute root directory of the Spotify Lakehouse repository."""
    env_root = os.getenv("SPOTIFY_PROJECT_ROOT")
    if env_root and Path(env_root).exists():
        return Path(env_root).resolve()

    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "dbt_spotify").exists() and ((parent / "data_quality").exists() or (parent / "member4").exists()):
            return parent
        if (parent / "ingestion").exists() and (parent / "spark").exists():
            return parent
        if (parent / "data_quality").exists() and (parent / "data").exists():
            return parent
    return current.parents[2]


PROJECT_ROOT = get_project_root()
DATA_QUALITY_DIR = (
    PROJECT_ROOT / "data_quality"
    if (PROJECT_ROOT / "data_quality").exists()
    else Path(__file__).resolve().parents[1]
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(DATA_QUALITY_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_QUALITY_DIR))

try:
    from data_quality.airflow.dags.spotify_pipeline import (
        task_generate_reports,
        task_ingest_dataset,
        task_preflight_check,
        task_run_dbt_command,
        task_run_spark_stage,
        task_validate_quality_gate,
    )
    from data_quality.monitoring.alert_handler import alert_failure_callback
    from data_quality.monitoring.metrics_collector import PipelineMetricsCollector
except ImportError:
    from member4.airflow.dags.spotify_pipeline import (
        task_generate_reports,
        task_ingest_dataset,
        task_preflight_check,
        task_run_dbt_command,
        task_run_spark_stage,
        task_validate_quality_gate,
    )
    from member4.monitoring.alert_handler import alert_failure_callback
    from member4.monitoring.metrics_collector import PipelineMetricsCollector


def run_local_pipeline(task_filter: str | None = None) -> int:
    print("\n" + "=" * 80)
    print("   SPOTIFY LAKEHOUSE - DATA QUALITY AIRFLOW WORKFLOW ORCHESTRATION RUNNER")
    print("=" * 80 + "\n")

    collector = PipelineMetricsCollector()
    stages = [
        ("preflight_health_check", task_preflight_check, {}),
        ("ingest_tracks", task_ingest_dataset, {"dataset_name": "tracks"}),
        ("ingest_charts", task_ingest_dataset, {"dataset_name": "charts"}),
        ("ingest_countries", task_ingest_dataset, {"dataset_name": "countries"}),
        ("ingest_artists", task_ingest_dataset, {"dataset_name": "artists"}),
        ("validate_bronze_quality_gate", task_validate_quality_gate, {"layer": "bronze"}),
        ("spark_bronze_to_silver", task_run_spark_stage, {"stage_name": "bronze_to_silver"}),
        ("validate_silver_quality_gate", task_validate_quality_gate, {"layer": "silver"}),
        ("spark_silver_to_gold", task_run_spark_stage, {"stage_name": "silver_to_gold"}),
        ("validate_gold_quality_gate", task_validate_quality_gate, {"layer": "gold"}),
        ("spark_verify_reconciliation", task_run_spark_stage, {"stage_name": "verify"}),
        ("dbt_build", task_run_dbt_command, {"command": "build"}),
        ("dbt_test", task_run_dbt_command, {"command": "test"}),
        ("dbt_export_marts", task_run_dbt_command, {"command": "export"}),
        ("validate_referential_integrity", task_validate_quality_gate, {"layer": "marts"}),
        ("generate_data_docs_and_monitoring_reports", task_generate_reports, {}),
    ]

    executed = 0
    failed = 0

    for name, func, kwargs in stages:
        if task_filter and task_filter not in name:
            continue

        print(f"\n---> [AIRFLOW TASK RUNNING]: {name}")
        t0 = time.time()
        status = "SUCCESS"
        details = {}
        try:
            res = func(**kwargs)
            if isinstance(res, dict):
                details = res
        except Exception as exc:
            status = "FAILED"
            failed += 1
            print(f"[AIRFLOW TASK FAILED]: {name} -> {exc}", file=sys.stderr)
            alert_failure_callback({
                "task_id": name,
                "dag_id": "spotify_lakehouse_pipeline",
                "exception": str(exc),
            })
            collector.record_task(name, status, time.time() - t0, extra_details={"error": str(exc)})
            print(f"\n[CRITICAL PIPELINE BLOCK]: Task '{name}' failed. Blocking downstream tasks.\n", file=sys.stderr)
            break

        duration = time.time() - t0
        collector.record_task(name, status, duration, extra_details=details)
        executed += 1
        print(f"---> [AIRFLOW TASK COMPLETED]: {name} in {duration:.2f}s [STATUS: {status}]")

    summary_file = collector.write_summary()
    print("\n" + "=" * 80)
    print(f"Airflow execution finished: {executed} tasks executed, {failed} failed.")
    print(f"Metrics saved to: {summary_file}")
    print("=" * 80 + "\n")

    return 1 if failed > 0 else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Data Quality Spotify Lakehouse Airflow pipeline tasks locally")
    parser.add_argument("--task", help="Optional task name filter", default=None)
    args = parser.parse_args()
    return run_local_pipeline(task_filter=args.task)


if __name__ == "__main__":
    sys.exit(main())
