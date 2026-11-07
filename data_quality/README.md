# Data Quality & Workflow Orchestration (Member 4)

Welcome to the isolated module for **Data Quality & Workflow Orchestration** (Member 4) of the Global Spotify Music Streaming Analytics Lakehouse capstone project.

---

## 1. Responsibilities & Deliverables

- **Great Expectations:** 11 expectation suites, native GX API execution, automated checkpoints, and interactive HTML Data Docs.
- **Apache Airflow:** Production DAG (`spotify_lakehouse_pipeline`) with strict quality gates, failure propagation, 3 retries with exponential backoffs, and 45-minute timeout.
- **Monitoring & Alerting:** `on_failure_callback`, `on_retry_callback`, `on_success_callback`, and quarantine budget sentinel (0.1% threshold).
- **Reports:** Automated HTML, Markdown, and JSON validation reports strictly in `data_quality/reports/`.
- **Isolated Tests:** Comprehensive test suite in `data_quality/tests/` verifying DAG integrity, GX execution, failure propagation, and alerts.

---

## 2. Directory Layout

```text
data_quality/
├── quality/
│   ├── expectations/              # 11 Official JSON Expectation Suites
│   ├── checkpoints/               # YAML Checkpoints for Bronze, Silver, Gold, & End-to-End
│   ├── validator.py               # Great Expectations execution engine (GX native API)
│   ├── expectations_definitions.py# Declarative definitions & supported expectation registry
│   └── report_generator.py        # HTML Data Docs & Markdown report builder
│
├── airflow/
│   ├── dags/
│   │   └── spotify_pipeline.py    # Production Airflow DAG with TaskGroups & quality gates
│   ├── dag_runner.py              # Local CLI execution runner
│   ├── docker-compose.yaml        # Standalone Airflow 2.10+ Docker stack
│   └── Dockerfile                 # Container definition with OpenJDK, PySpark, dbt, & GX
│
├── monitoring/
│   ├── alert_handler.py           # Callbacks, webhook dispatcher, & quarantine breach sentinel
│   └── metrics_collector.py       # Task duration and audit metrics logger
│
├── tests/                         # Isolated Data Quality tests
│   ├── conftest.py                # Isolated test fixtures
│   ├── test_great_expectations.py # GX API execution & anomaly tests
│   ├── test_airflow_dag.py        # DAG integrity & failure blocking tests
│   ├── test_monitoring.py         # Callbacks & breach sentinel tests
│   └── test_end_to_end_scenarios.py # End-to-end scenarios (success & controlled failure)
│
├── reports/                       # Generated audit artifacts
│   ├── great_expectations_report.html
│   ├── data_quality_summary.md
│   ├── great_expectations_validation.json
│   ├── pipeline_metrics.json
│   └── monitoring_alerts.log
│
├── docs/                          # Member 4 documentation
│   ├── data_quality_and_orchestration.md
│   ├── MEMBER_4_VIVA.md           # Complete viva preparation guide
│   └── WSL_SETUP_GUIDE.md         # Ubuntu 24.04 WSL setup instructions
│
├── run_pipeline.sh                # Bash entry point for Ubuntu 24.04 WSL
├── requirements.txt               # Pinned dependencies
└── README.md
```

---

## 3. Quickstart & Verification

### Run Isolated Tests
```bash
python -m pytest data_quality/tests/ -v
```

### Run Great Expectations Validation
```bash
python -m data_quality.quality.validator --layer all
```

### Run Airflow DAG Locally
```bash
python data_quality/airflow/dag_runner.py
```
