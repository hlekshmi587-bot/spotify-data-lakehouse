#!/usr/bin/env bash
# ==============================================================================
# Spotify Lakehouse: Data Quality End-to-End Execution Script (Ubuntu 24.04 WSL)
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
export PYTHONPATH="${PROJECT_ROOT}:${SCRIPT_DIR}:${PYTHONPATH}"

echo "========================================================================"
echo "   SPOTIFY DATA LAKEHOUSE - DATA QUALITY PIPELINE EXECUTION (WSL)"
echo "========================================================================"

echo "[1/3] Running Data Quality Isolated Test Suite..."
python3 -m pytest "${SCRIPT_DIR}/tests/" -v

echo "[2/3] Executing Airflow Pipeline Tasks..."
python3 "${SCRIPT_DIR}/airflow/dag_runner.py"

echo "[3/3] Generating Great Expectations Data Docs & Reports..."
python3 -m data_quality.quality.validator --layer all

echo "========================================================================"
echo "   DATA QUALITY EXECUTION COMPLETE"
echo "   Reports generated in: ${SCRIPT_DIR}/reports/"
echo "========================================================================"
