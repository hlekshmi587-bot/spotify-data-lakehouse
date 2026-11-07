"""Isolated tests for Data Quality Airflow DAG structure, task invocations, and failure propagation."""
from __future__ import annotations

import unittest
from datetime import timedelta
from unittest import mock
import pandas as pd

from data_quality.airflow.dags.spotify_pipeline import (
    dag,
    default_args,
    task_ingest_dataset,
    task_run_dbt_command,
    task_run_spark_stage,
    task_validate_quality_gate,
)


class TestDataQualityAirflowDAG(unittest.TestCase):
    def test_dag_loaded_successfully(self):
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "spotify_lakehouse_pipeline")

    def test_default_args(self):
        self.assertEqual(default_args.get("owner"), "member4_orchestration")
        self.assertEqual(default_args.get("retries"), 3)
        self.assertTrue(default_args.get("retry_exponential_backoff"))
        self.assertEqual(default_args.get("retry_delay"), timedelta(minutes=2))
        self.assertEqual(default_args.get("execution_timeout"), timedelta(minutes=45))
        self.assertIsNotNone(default_args.get("on_failure_callback"))
        self.assertIsNotNone(default_args.get("on_retry_callback"))
        self.assertIsNotNone(default_args.get("on_success_callback"))

    def test_required_pipeline_tasks_exist(self):
        expected_tasks = [
            "start", "preflight_health_check", "ingest_tracks", "ingest_charts",
            "ingest_countries", "ingest_artists", "validate_bronze_quality_gate",
            "spark_bronze_to_silver", "validate_silver_quality_gate",
            "spark_silver_to_gold", "validate_gold_quality_gate",
            "spark_verify_reconciliation", "dbt_build", "dbt_test",
            "dbt_export_marts", "validate_referential_integrity",
            "generate_data_docs_and_monitoring_reports", "end",
        ]
        actual_tasks = list(dag.tasks.keys()) if hasattr(dag, "tasks") else list(dag.task_dict.keys())
        for task_id in expected_tasks:
            self.assertIn(task_id, actual_tasks, f"Task '{task_id}' missing from Airflow DAG")

    def test_ingestion_functions_actually_invoked(self):
        """CRITICAL FIX: Verify actual ingestion entry points are called."""
        mock_track_mod = mock.MagicMock()
        mock_track_mod.run_track_ingestion.return_value = True
        mock_chart_mod = mock.MagicMock()
        mock_chart_mod.run_chart_ingestion.return_value = True
        mock_country_mod = mock.MagicMock()
        mock_country_mod.run_country_codes_ingestion.return_value = True
        mock_artist_mod = mock.MagicMock()
        mock_artist_mod.ingest_artists.return_value = None

        with mock.patch.dict("sys.modules", {
            "ingestion": mock.MagicMock(),
            "ingestion.track_ingestion": mock_track_mod,
            "ingestion.chart_ingestion": mock_chart_mod,
            "ingestion.country_codes_ingestion": mock_country_mod,
            "ingestion.artist_ingestion": mock_artist_mod,
        }):
            res_tracks = task_ingest_dataset("tracks")
            self.assertEqual(res_tracks["status"], "SUCCESS")
            mock_track_mod.run_track_ingestion.assert_called_once()

            res_charts = task_ingest_dataset("charts")
            self.assertEqual(res_charts["status"], "SUCCESS")
            mock_chart_mod.run_chart_ingestion.assert_called_once()

            res_countries = task_ingest_dataset("countries")
            self.assertEqual(res_countries["status"], "SUCCESS")
            mock_country_mod.run_country_codes_ingestion.assert_called_once()

            res_artists = task_ingest_dataset("artists")
            self.assertEqual(res_artists["status"], "SUCCESS")
            mock_artist_mod.ingest_artists.assert_called_once()

    def test_ingestion_failure_propagates_exception(self):
        """When an ingestion function returns failure, task must raise RuntimeError."""
        mock_track_mod = mock.MagicMock()
        mock_track_mod.run_track_ingestion.return_value = False

        with mock.patch.dict("sys.modules", {
            "ingestion": mock.MagicMock(),
            "ingestion.track_ingestion": mock_track_mod,
        }):
            with self.assertRaises(RuntimeError) as ctx:
                task_ingest_dataset("tracks")
            self.assertIn("track ingestion returned failure status", str(ctx.exception))

    def test_spark_missing_or_failed_raises_exception(self):
        """CRITICAL FIX: Spark stage must fail when PySpark or job fails."""
        with self.assertRaises(RuntimeError) as ctx:
            task_run_spark_stage("bronze_to_silver")
        self.assertTrue(
            "Cannot execute Spark stage" in str(ctx.exception)
            or "PySpark stage 'bronze_to_silver' failed" in str(ctx.exception)
        )

    def test_dbt_failure_propagates_exception(self):
        """CRITICAL FIX: dbt command must raise when returncode != 0."""
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            mock_run.return_value.stderr = "Compilation Error in model stg_tracks"
            mock_run.return_value.stdout = ""

            with self.assertRaises(RuntimeError) as ctx:
                task_run_dbt_command("build")
            self.assertIn("dbt command 'build' failed with exit code 1", str(ctx.exception))

    def test_failed_validation_raises_exception_and_blocks_downstream(self):
        """CRITICAL FIX: Failed quality gate MUST raise ValueError to block downstream tasks."""
        bad_df = pd.DataFrame({
            "track_id": ["not_22_chars"],
            "popularity": [999],
        })

        from data_quality.quality.validator import GreatExpectationsValidator
        validator = GreatExpectationsValidator()
        res = validator.validate_suite("silver_tracks_suite", bad_df, "silver", "tracks")
        self.assertFalse(res.success)

        with self.assertRaises(ValueError) as ctx:
            with mock.patch("data_quality.quality.validator.GreatExpectationsValidator.run_layer_validation") as mock_val:
                mock_val.return_value = [res]
                task_validate_quality_gate("silver")
        self.assertIn("Quality Gate FAILED for layer 'silver'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
