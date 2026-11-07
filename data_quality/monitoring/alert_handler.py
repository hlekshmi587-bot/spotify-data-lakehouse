"""Pipeline monitoring, SLA tracking, and failure alerting for Data Quality.

Stores all logs and monitoring artifacts strictly inside data_quality/reports/.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def get_data_quality_dir() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "quality").exists() and (parent / "monitoring").exists() and parent.name == "data_quality":
            return parent
    return current.parents[1]


DATA_QUALITY_DIR = get_data_quality_dir()
REPORTS_DIR = DATA_QUALITY_DIR / "reports"
ALERTS_LOG = REPORTS_DIR / "monitoring_alerts.log"
MONITORING_JSON = REPORTS_DIR / "pipeline_monitoring.json"

logger = logging.getLogger("data_quality_monitoring")
logger.setLevel(logging.INFO)


def _ensure_dirs() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _append_alert_log(event_type: str, details: dict[str, Any]) -> None:
    _ensure_dirs()
    timestamp = datetime.now(timezone.utc).isoformat()
    entry = f"[{timestamp}] [{event_type.upper()}] {json.dumps(details)}\n"
    with open(ALERTS_LOG, "a", encoding="utf-8") as f:
        f.write(entry)


def _update_monitoring_json(entry: dict[str, Any]) -> None:
    _ensure_dirs()
    data: dict[str, Any] = {"history": []}
    if MONITORING_JSON.exists():
        try:
            data = json.loads(MONITORING_JSON.read_text(encoding="utf-8"))
        except Exception:
            data = {"history": []}

    data["last_updated_utc"] = datetime.now(timezone.utc).isoformat()
    data["history"].append(entry)
    data["history"] = data["history"][-100:]
    MONITORING_JSON.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def alert_failure_callback(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback invoked when a task fails."""
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else context.get("task_id", "unknown_task")
    dag_id = ti.dag_id if ti else context.get("dag_id", "unknown_dag")
    execution_date = str(context.get("execution_date", datetime.now(timezone.utc)))
    exception = str(context.get("exception", "Unspecified Exception"))
    try_number = ti.try_number if ti else 1

    alert_payload = {
        "event": "TASK_FAILURE",
        "dag_id": str(dag_id),
        "task_id": str(task_id),
        "try_number": try_number,
        "execution_date": execution_date,
        "exception": exception,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "action_required": f"Inspect logs for task '{task_id}' in DAG '{dag_id}'. Check Great Expectations quality gate or upstream inputs.",
    }

    _append_alert_log("TASK_FAILURE", alert_payload)
    _update_monitoring_json(alert_payload)

    print(f"\n[AIRFLOW MONITORING ALERT - FAILURE] Task '{task_id}' failed in DAG '{dag_id}' (attempt {try_number})!", file=sys.stderr)
    print(f"Exception: {exception}\n", file=sys.stderr)

    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL")
    if webhook_url:
        try:
            import urllib.request
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps({"text": f":rotating_light: Airflow Alert: Task `{task_id}` failed: {exception}"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            logger.warning("Could not dispatch webhook alert: %s", e)


def alert_retry_callback(context: dict[str, Any]) -> None:
    """Airflow on_retry_callback invoked when a task retries."""
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else context.get("task_id", "unknown_task")
    dag_id = ti.dag_id if ti else context.get("dag_id", "unknown_dag")
    try_number = ti.try_number if ti else 1

    retry_payload = {
        "event": "TASK_RETRY",
        "dag_id": str(dag_id),
        "task_id": str(task_id),
        "retry_attempt": try_number,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    _append_alert_log("TASK_RETRY", retry_payload)
    _update_monitoring_json(retry_payload)
    print(f"[AIRFLOW MONITORING - RETRY] Task '{task_id}' retrying (attempt {try_number})...")


def alert_success_callback(context: dict[str, Any]) -> None:
    """Airflow on_success_callback invoked when a task succeeds."""
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else context.get("task_id", "unknown_task")
    dag_id = ti.dag_id if ti else context.get("dag_id", "unknown_dag")

    success_payload = {
        "event": "TASK_SUCCESS",
        "dag_id": str(dag_id),
        "task_id": str(task_id),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    _append_alert_log("TASK_SUCCESS", success_payload)
    _update_monitoring_json(success_payload)


def alert_quarantine_breach(
    dataset_name: str,
    quarantine_count: int,
    total_count: int,
    budget_pct: float = 0.1,
) -> bool:
    """Detect if quarantined error records exceed the allowed budget percentage."""
    if total_count <= 0:
        return False
    actual_pct = round((quarantine_count / total_count * 100), 4)
    if actual_pct > budget_pct:
        breach_payload = {
            "event": "QUARANTINE_BUDGET_BREACH",
            "dataset": dataset_name,
            "quarantined_count": quarantine_count,
            "total_records": total_count,
            "actual_error_pct": actual_pct,
            "budget_threshold_pct": budget_pct,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "severity": "CRITICAL",
        }
        _append_alert_log("QUARANTINE_BREACH", breach_payload)
        _update_monitoring_json(breach_payload)
        print(f"\n[CRITICAL QUALITY ALERT] Quarantine budget breach in '{dataset_name}': {actual_pct}% > {budget_pct}% budget!", file=sys.stderr)
        return True
    return False
