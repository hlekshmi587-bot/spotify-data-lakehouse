"""Apache Airflow DAG: Spotify Music Streaming Data Lakehouse Pipeline.

Author: Member 4 (Data Quality & Workflow Orchestration)
Role: Automates ingestion, Spark ETL, Great Expectations quality gates, dbt marts,
      exports, and pipeline monitoring with comprehensive failure handling.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


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

    if len(current.parents) >= 4 and current.parents[2].name in ("data_quality", "member4"):
        return current.parents[3]
    return current.parents[2]


PROJECT_ROOT = get_project_root()
DATA_QUALITY_DIR = (
    PROJECT_ROOT / "data_quality"
    if (PROJECT_ROOT / "data_quality").exists()
    else Path(__file__).resolve().parents[2]
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(DATA_QUALITY_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_QUALITY_DIR))

try:
    from data_quality.monitoring.alert_handler import (
        alert_failure_callback,
        alert_retry_callback,
        alert_success_callback,
    )
except ImportError:
    from member4.monitoring.alert_handler import (
        alert_failure_callback,
        alert_retry_callback,
        alert_success_callback,
    )

# --------------------------------------------------------------------------- #
# Compatibility layer: allows DAG definition & testing even without Airflow installed
# --------------------------------------------------------------------------- #
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    _ACTIVE_DAG = None

    class DAG:
        def __init__(self, dag_id: str, **kwargs):
            self.dag_id = dag_id
            self.default_args = kwargs.get("default_args", {})
            self.description = kwargs.get("description", "")
            self.tasks: dict[str, Any] = {}

        def __enter__(self):
            global _ACTIVE_DAG
            _ACTIVE_DAG = self
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            global _ACTIVE_DAG
            _ACTIVE_DAG = None

        def add_task(self, task: Any) -> None:
            self.tasks[task.task_id] = task

        @property
        def task_dict(self):
            return self.tasks

    class BaseOperator:
        def __init__(self, task_id: str, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.upstream_list = []
            self.downstream_list = []
            if _ACTIVE_DAG is not None:
                _ACTIVE_DAG.add_task(self)

        def __rshift__(self, other):
            if isinstance(other, list):
                for o in other:
                    self.__rshift__(o)
                return other
            if isinstance(other, TaskGroup):
                other.set_upstream(self)
                return other
            other.upstream_list.append(self)
            self.downstream_list.append(other)
            return other

        def __rrshift__(self, other):
            if isinstance(other, list):
                for o in other:
                    o.__rshift__(self)
                return self
            return self

    class EmptyOperator(BaseOperator):
        pass

    class PythonOperator(BaseOperator):
        def __init__(self, task_id: str, python_callable: Any, op_kwargs: dict[str, Any] | None = None, **kwargs):
            super().__init__(task_id, **kwargs)
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}

        def execute(self, context=None):
            return self.python_callable(**(self.op_kwargs or {}))

    class BashOperator(BaseOperator):
        def __init__(self, task_id: str, bash_command: str, **kwargs):
            super().__init__(task_id, **kwargs)
            self.bash_command = bash_command

    class TaskGroup:
        def __init__(self, group_id: str, **kwargs):
            self.group_id = group_id
            self.children: list[BaseOperator] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def set_upstream(self, other):
            if self.children:
                other >> self.children[0]

        def __rshift__(self, other):
            if self.children:
                self.children[-1] >> other
            return other

        def __rrshift__(self, other):
            if self.children:
                other >> self.children[0]
            return self


# --------------------------------------------------------------------------- #
# Default Arguments & SLA Configuration
# --------------------------------------------------------------------------- #
default_args = {
    "owner": "member4_orchestration",
    "depends_on_past": False,
    "email": ["data-quality-alerts@spotify-lakehouse.internal"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": alert_failure_callback,
    "on_retry_callback": alert_retry_callback,
    "on_success_callback": alert_success_callback,
}


# --------------------------------------------------------------------------- #
# Task Callables with Strict Failure Propagation
# --------------------------------------------------------------------------- #

def task_preflight_check(**context) -> dict[str, Any]:
    """Verify system health, input folders, and configurations."""
    print("=== [AIRFLOW PREFLIGHT HEALTH CHECK] ===")
    data_dir = Path(os.getenv("SPOTIFY_DATA_DIR", PROJECT_ROOT / "data"))

    if not PROJECT_ROOT.exists():
        raise RuntimeError(f"Project root does not exist: {PROJECT_ROOT}")

    checks = {
        "project_root": str(PROJECT_ROOT),
        "data_dir": str(data_dir),
        "python_version": sys.version.split()[0],
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    print(f"Preflight health status: {checks}")
    return checks


def task_ingest_dataset(dataset_name: str, **context) -> dict[str, Any]:
    """Execute Member 1 ingestion modules with strict error detection.

    Invokes actual callable entry points:
    - tracks: ingestion.track_ingestion.run_track_ingestion()
    - charts: ingestion.chart_ingestion.run_chart_ingestion()
    - countries: ingestion.country_codes_ingestion.run_country_codes_ingestion()
    - artists: ingestion.artist_ingestion.ingest_artists()
    """
    print(f"=== [AIRFLOW INGESTION: {dataset_name.upper()}] ===")
    t0 = time.time()

    if dataset_name == "tracks":
        from ingestion.track_ingestion import run_track_ingestion
        success = run_track_ingestion()
        if not success:
            raise RuntimeError(f"Member 1 track ingestion returned failure status for '{dataset_name}'")
    elif dataset_name == "charts":
        from ingestion.chart_ingestion import run_chart_ingestion
        success = run_chart_ingestion()
        if not success:
            raise RuntimeError(f"Member 1 chart ingestion returned failure status for '{dataset_name}'")
    elif dataset_name == "countries":
        from ingestion.country_codes_ingestion import run_country_codes_ingestion
        success = run_country_codes_ingestion()
        if not success:
            raise RuntimeError(f"Member 1 country codes ingestion returned failure status for '{dataset_name}'")
    elif dataset_name == "artists":
        from ingestion.artist_ingestion import ingest_artists
        ingest_artists()
    else:
        raise ValueError(f"Unknown ingestion dataset: '{dataset_name}'")

    duration = round(time.time() - t0, 2)
    return {"dataset": dataset_name, "duration_seconds": duration, "status": "SUCCESS"}


def task_validate_quality_gate(layer: str, **context) -> dict[str, Any]:
    """Execute Great Expectations quality gate. Raises on failure!"""
    print(f"\n========================================================")
    print(f"=== [AIRFLOW QUALITY GATE: GREAT EXPECTATIONS - {layer.upper()}] ===")
    print(f"========================================================\n")
    try:
        from data_quality.quality.validator import GreatExpectationsValidator
    except ImportError:
        from member4.quality.validator import GreatExpectationsValidator

    validator = GreatExpectationsValidator()
    results = validator.run_layer_validation(layer)

    if not results:
        raise ValueError(f"Quality gate error: No validation suites found for layer '{layer}'!")

    json_path = validator.save_validation_results(results)
    total_exp = sum(r.total_expectations for r in results)
    passed_exp = sum(r.passed_expectations for r in results)
    failed_suites = [r.suite_name for r in results if not r.success]
    missing_datasets = [r.target_asset for r in results if r.status == "MISSING_DATASET"]

    print(f"Layer '{layer}' Quality Gate: {passed_exp}/{total_exp} checks passed across {len(results)} suites.")

    if missing_datasets:
        raise ValueError(
            f"Quality Gate FAILED for layer '{layer}'! Missing required datasets: {missing_datasets}. "
            f"Halting downstream pipeline execution."
        )

    if failed_suites:
        raise ValueError(
            f"Quality Gate FAILED for layer '{layer}'! Suites with violations: {failed_suites}. "
            f"Halting downstream pipeline execution."
        )

    return {
        "layer": layer,
        "status": "PASSED",
        "total_expectations": total_exp,
        "passed_expectations": passed_exp,
        "report_path": str(json_path),
    }


def task_run_spark_stage(stage_name: str, **context) -> dict[str, Any]:
    """Execute Member 2 PySpark lakehouse processing stage with strict output verification."""
    print(f"=== [AIRFLOW PYSPARK STAGE: {stage_name.upper()}] ===")
    t0 = time.time()
    try:
        from spark.spark_session import get_spark
        from utils.config import PipelineConfig
    except ImportError as e:
        raise RuntimeError(
            f"PySpark or required dependencies are not installed ({e}). "
            f"Cannot execute Spark stage '{stage_name}'."
        ) from e

    try:
        config = PipelineConfig.from_env()
        spark = get_spark(config)

        if stage_name == "bronze_to_silver":
            from spark.bronze_to_silver import run_silver
            run_silver(spark, config)
            # Verify outputs exist
            for ds in ("tracks", "charts", "countries", "track_genres"):
                out_path = config.data_dir / "silver" / ds
                if not out_path.exists():
                    raise RuntimeError(f"Expected Silver dataset '{ds}' not found at {out_path}")

        elif stage_name == "silver_to_gold":
            from spark.silver_to_gold import run_gold
            run_gold(spark, config)
            for ds in ("enriched_chart_data", "country_analytics"):
                out_path = config.data_dir / "gold" / ds
                if not out_path.exists():
                    raise RuntimeError(f"Expected Gold dataset '{ds}' not found at {out_path}")

        elif stage_name == "verify":
            from spark.verify_outputs import run_verify
            res = run_verify(spark, config)
            failed_tables = [t.dataset for t in res.tables if t.rows_out == 0]
            if failed_tables:
                raise RuntimeError(f"Spark verification reported empty tables: {failed_tables}")

        else:
            raise ValueError(f"Unknown Spark stage: {stage_name}")

        duration = round(time.time() - t0, 2)
        return {"stage": stage_name, "status": "SUCCESS", "duration_seconds": duration}
    except Exception as exc:
        raise RuntimeError(f"PySpark stage '{stage_name}' failed: {exc}") from exc


def task_run_dbt_command(command: str, **context) -> dict[str, Any]:
    """Execute Member 3 dbt transformations, tests, or exports with returncode checks."""
    print(f"=== [AIRFLOW DBT COMMAND: {command.upper()}] ===")
    dbt_dir = PROJECT_ROOT / "dbt_spotify"
    t0 = time.time()

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(dbt_dir)

    if command == "build":
        cmd = [sys.executable, "-m", "dbt", "build"]
    elif command == "test":
        cmd = [sys.executable, "-m", "dbt", "test"]
    elif command == "export":
        cmd = [sys.executable, str(dbt_dir / "scripts" / "export_marts.py")]
    else:
        raise ValueError(f"Unknown dbt command: {command}")

    proc = subprocess.run(cmd, cwd=str(dbt_dir), env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        err_out = proc.stderr or proc.stdout
        print(f"dbt {command} error output:\n{err_out[:500]}", file=sys.stderr)
        raise RuntimeError(f"dbt command '{command}' failed with exit code {proc.returncode}: {err_out.strip()}")

    duration = round(time.time() - t0, 2)
    return {"command": command, "status": "SUCCESS", "duration_seconds": duration}


def task_generate_reports(**context) -> dict[str, Any]:
    """Compile final Great Expectations Data Docs & pipeline monitoring summaries."""
    print("=== [AIRFLOW REPORT GENERATION: DATA DOCS & MONITORING] ===")
    try:
        from data_quality.quality.report_generator import generate_validation_reports
        from data_quality.quality.validator import GreatExpectationsValidator
    except ImportError:
        from member4.quality.report_generator import generate_validation_reports
        from member4.quality.validator import GreatExpectationsValidator

    validator = GreatExpectationsValidator()
    results = validator.run_layer_validation("all")
    html_file = generate_validation_reports(results)
    json_file = validator.save_validation_results(results)
    print(f"Great Expectations Data Docs generated at: {html_file}")
    return {"status": "SUCCESS", "html_report": str(html_file), "json_report": str(json_file)}


# --------------------------------------------------------------------------- #
# DAG Topological Pipeline Declaration
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="spotify_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-End Spotify Lakehouse Orchestration with Great Expectations Quality Gates (Data Quality)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "lakehouse", "great_expectations", "airflow", "data_quality"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Preflight
    preflight = PythonOperator(
        task_id="preflight_health_check",
        python_callable=task_preflight_check,
    )

    # 2. Ingestion Group (Member 1)
    with TaskGroup("ingestion_group") as ingestion_group:
        ingest_tracks = PythonOperator(
            task_id="ingest_tracks",
            python_callable=task_ingest_dataset,
            op_kwargs={"dataset_name": "tracks"},
        )
        ingest_charts = PythonOperator(
            task_id="ingest_charts",
            python_callable=task_ingest_dataset,
            op_kwargs={"dataset_name": "charts"},
        )
        ingest_countries = PythonOperator(
            task_id="ingest_countries",
            python_callable=task_ingest_dataset,
            op_kwargs={"dataset_name": "countries"},
        )
        ingest_artists = PythonOperator(
            task_id="ingest_artists",
            python_callable=task_ingest_dataset,
            op_kwargs={"dataset_name": "artists"},
        )

    # 3. Bronze Quality Gate (Data Quality)
    validate_bronze = PythonOperator(
        task_id="validate_bronze_quality_gate",
        python_callable=task_validate_quality_gate,
        op_kwargs={"layer": "bronze"},
    )

    # 4. Spark ETL (Member 2) + Silver/Gold Quality Gates (Data Quality)
    with TaskGroup("spark_lakehouse_group") as spark_group:
        spark_bronze_to_silver = PythonOperator(
            task_id="spark_bronze_to_silver",
            python_callable=task_run_spark_stage,
            op_kwargs={"stage_name": "bronze_to_silver"},
        )
        validate_silver = PythonOperator(
            task_id="validate_silver_quality_gate",
            python_callable=task_validate_quality_gate,
            op_kwargs={"layer": "silver"},
        )
        spark_silver_to_gold = PythonOperator(
            task_id="spark_silver_to_gold",
            python_callable=task_run_spark_stage,
            op_kwargs={"stage_name": "silver_to_gold"},
        )
        validate_gold = PythonOperator(
            task_id="validate_gold_quality_gate",
            python_callable=task_validate_quality_gate,
            op_kwargs={"layer": "gold"},
        )
        spark_verify = PythonOperator(
            task_id="spark_verify_reconciliation",
            python_callable=task_run_spark_stage,
            op_kwargs={"stage_name": "verify"},
        )

        spark_bronze_to_silver >> validate_silver >> spark_silver_to_gold >> validate_gold >> spark_verify

    # 5. dbt Transformation & Marts (Member 3)
    with TaskGroup("dbt_marts_group") as dbt_group:
        dbt_build = PythonOperator(
            task_id="dbt_build",
            python_callable=task_run_dbt_command,
            op_kwargs={"command": "build"},
        )
        dbt_test = PythonOperator(
            task_id="dbt_test",
            python_callable=task_run_dbt_command,
            op_kwargs={"command": "test"},
        )
        dbt_export = PythonOperator(
            task_id="dbt_export_marts",
            python_callable=task_run_dbt_command,
            op_kwargs={"command": "export"},
        )

        dbt_build >> dbt_test >> dbt_export

    # 6. Referential Integrity & Final Reporting (Data Quality)
    validate_integrity = PythonOperator(
        task_id="validate_referential_integrity",
        python_callable=task_validate_quality_gate,
        op_kwargs={"layer": "marts"},
    )

    generate_reports = PythonOperator(
        task_id="generate_data_docs_and_monitoring_reports",
        python_callable=task_generate_reports,
    )

    # Strict Topological Flow with Quality Gates
    start >> preflight >> ingestion_group >> validate_bronze >> spark_group >> dbt_group >> validate_integrity >> generate_reports >> end
