"""Pipeline metrics collection and execution performance tracking for Data Quality.

Saves execution summaries to data_quality/reports/pipeline_metrics.json.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
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
METRICS_FILE = DATA_QUALITY_DIR / "reports" / "pipeline_metrics.json"


@dataclass
class TaskMetric:
    task_id: str
    status: str
    duration_seconds: float
    records_in: int | None = None
    records_out: int | None = None
    records_quarantined: int | None = None
    quality_pass_rate: float | None = None
    extra_details: dict[str, Any] = field(default_factory=dict)


class PipelineMetricsCollector:
    def __init__(self, run_id: str | None = None):
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.start_time = time.time()
        self.task_metrics: list[TaskMetric] = []

    def record_task(
        self,
        task_id: str,
        status: str,
        duration_seconds: float,
        records_in: int | None = None,
        records_out: int | None = None,
        records_quarantined: int | None = None,
        quality_pass_rate: float | None = None,
        extra_details: dict[str, Any] | None = None,
    ) -> None:
        metric = TaskMetric(
            task_id=task_id,
            status=status,
            duration_seconds=round(duration_seconds, 2),
            records_in=records_in,
            records_out=records_out,
            records_quarantined=records_quarantined,
            quality_pass_rate=quality_pass_rate,
            extra_details=extra_details or {},
        )
        self.task_metrics.append(metric)

    def write_summary(self) -> Path:
        total_duration = round(time.time() - self.start_time, 2)
        payload = {
            "run_id": self.run_id,
            "completed_utc": datetime.now(timezone.utc).isoformat(),
            "total_duration_seconds": total_duration,
            "overall_status": "SUCCESS" if all(m.status == "SUCCESS" for m in self.task_metrics) else "FAILED",
            "task_count": len(self.task_metrics),
            "tasks": [asdict(m) for m in self.task_metrics],
        }
        METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        METRICS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return METRICS_FILE
