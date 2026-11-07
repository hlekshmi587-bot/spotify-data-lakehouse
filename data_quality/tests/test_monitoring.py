"""Isolated tests for Data Quality pipeline monitoring, SLA alerting, and metrics."""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from data_quality.monitoring.alert_handler import (
    ALERTS_LOG,
    MONITORING_JSON,
    alert_failure_callback,
    alert_quarantine_breach,
    alert_retry_callback,
    alert_success_callback,
)
from data_quality.monitoring.metrics_collector import PipelineMetricsCollector


class TestDataQualityMonitoring(unittest.TestCase):
    def test_alert_callbacks_write_to_data_quality_reports(self):
        mock_context = {
            "task_id": "test_spark_bronze_to_silver",
            "dag_id": "spotify_lakehouse_pipeline",
            "execution_date": "2026-11-05T00:00:00Z",
            "exception": "Simulated Connection Timeout in Spark",
        }
        alert_failure_callback(mock_context)
        alert_retry_callback(mock_context)
        alert_success_callback(mock_context)

        self.assertTrue(ALERTS_LOG.exists())
        self.assertIn("data_quality", str(ALERTS_LOG))

        log_content = ALERTS_LOG.read_text(encoding="utf-8")
        self.assertIn("TASK_FAILURE", log_content)
        self.assertIn("test_spark_bronze_to_silver", log_content)

        self.assertTrue(MONITORING_JSON.exists())
        json_data = json.loads(MONITORING_JSON.read_text(encoding="utf-8"))
        self.assertIn("history", json_data)

    def test_quarantine_budget_sentinel(self):
        # 5 out of 10,000 = 0.05% <= 0.1% -> False (no breach)
        self.assertFalse(alert_quarantine_breach("tracks", 5, 10000, 0.1))
        # 25 out of 10,000 = 0.25% > 0.1% -> True (critical breach)
        self.assertTrue(alert_quarantine_breach("tracks", 25, 10000, 0.1))

    def test_metrics_collector_summary(self):
        collector = PipelineMetricsCollector(run_id="test_dq_run")
        collector.record_task("preflight", "SUCCESS", 0.05)
        collector.record_task("validate_silver", "SUCCESS", 1.20, records_in=89740, records_out=89740)
        out_file = collector.write_summary()

        self.assertTrue(out_file.exists())
        self.assertIn("data_quality", str(out_file))

        data = json.loads(out_file.read_text(encoding="utf-8"))
        self.assertEqual(data["run_id"], "test_dq_run")
        self.assertEqual(data["overall_status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
