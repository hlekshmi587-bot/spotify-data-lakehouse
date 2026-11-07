# Ubuntu 24.04 WSL Setup & Reproducibility Guide (Data Quality)

This guide provides reproducible steps to set up and run Data Quality deliverables on **Ubuntu 24.04 LTS (WSL2)**.

---

## 1. System Prerequisites

On Ubuntu 24.04:
```bash
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv openjdk-17-jre-headless curl git
```

Verify Java and Python versions:
```bash
python3 --version   # Python 3.10+ / 3.12+
java -version       # OpenJDK 17 or 21
```

---

## 2. Python Environment Setup

Navigate to the project root and create a dedicated virtual environment:
```bash
cd /path/to/spotify-data-lakehouse
python3 -m venv .venv_dq
source .venv_dq/bin/activate

# Install Data Quality dependencies
pip install -r data_quality/requirements.txt
```

---

## 3. Running the Pipeline & Quality Gates

### Option A: Using the Automated Bash Script
```bash
chmod +x data_quality/run_pipeline.sh
./data_quality/run_pipeline.sh
```

### Option B: Running Specific Stages via Local DAG Runner
```bash
# Run all tasks in topological order
python3 data_quality/airflow/dag_runner.py

# Run only quality gates
python3 data_quality/airflow/dag_runner.py --task validate
```

### Option C: Running Great Expectations Directly
```bash
# Validate all layers and generate HTML Data Docs
python3 -m data_quality.quality.validator --layer all
```

---

## 4. Running the Isolated Test Suite

Execute all 22 tests verifying DAG, GX suites, monitoring, and failure blocking:
```bash
pytest data_quality/tests/ -v
```

All reports and metrics are written directly to:
```
data_quality/reports/
├── great_expectations_report.html
├── data_quality_summary.md
├── great_expectations_validation.json
├── pipeline_metrics.json
└── monitoring_alerts.log
```
