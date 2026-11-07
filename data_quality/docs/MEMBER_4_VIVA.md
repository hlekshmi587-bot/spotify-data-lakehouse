# MEMBER 4 - Viva Preparation Guide (Data Quality & Workflow Orchestration)

**My Official Contribution:** *"I implemented data-quality validation using Great Expectations and automated the pipeline using Airflow."*

---

## 30-Second Elevator Pitch

> *"In our Spotify Lakehouse project, I owned **Data Quality Validation** and **End-to-End Workflow Orchestration**.
> 
> For data quality, I designed and implemented **11 Great Expectations suites** acting as automated quality gates across Bronze, Silver, Gold, and Marts layers. I validated schema conformity, null constraints, primary key uniqueness, audio feature ranges, ISO dates, chart rank boundaries, and referential integrity—including enforcing that bridge weights sum strictly to 1.0. I also implemented a **quarantine error isolation pattern** with an automated budget threshold (0.1%), ensuring bad records are isolated with documented reasons without crashing the lakehouse.
> 
> For orchestration, I built our production **Apache Airflow DAG** (`spotify_lakehouse_pipeline`) using modular TaskGroups to coordinate Member 1's ingestion, Member 2's PySpark ETL, Member 3's dbt transformations, and Member 5's exports. The DAG enforces SLA timeouts (45 min), 3 retries with exponential backoff, failure alert callbacks, and automated generation of interactive HTML Data Docs."*

---

## Part 1 - The Critical Question: dbt Tests vs. Great Expectations Tests

> **Examiner Question:** *"Member 3 already wrote 102 dbt tests. Why did your team also need Member 4's Great Expectations tests? Isn't that redundant?"*

**Answer:**
> *"They are completely complementary because they operate at different layers, on different technologies, and at different lifecycle moments:
> 
> 1. **Data Layer vs. Analytical Model:** Member 3's dbt tests run inside the SQL warehouse (DuckDB/Postgres) against dimensional tables *after* models are built. Great Expectations validates the **physical files in the Lakehouse layers (Bronze, Silver, Gold Parquet files)** *before* and *after* Spark runs.
> 2. **Quality Gates in Pipeline Flow:** Airflow uses Great Expectations as **pre-transformation quality gates**. If Bronze or Silver fails quality gates, Airflow aborts the run *before* spinning up expensive dbt or warehouse compute.
> 3. **Validation Capabilities:** Great Expectations handles distributed file checks, regex schema matching, and quarantine error budgets that cannot be evaluated within simple SQL `ref()` tests.
> 4. **Business & Compliance Reporting:** Great Expectations generates interactive HTML **Data Docs** showing executive KPIs, audit-ready timestamps, pass/fail percentages, and violation diagnostics for data governance."*

---

## Part 2 - Numbers You Must Know Cold

| Metric | Value | Significance |
|---|---|---|
| **Great Expectations Suites** | **11 suites** | Bronze (3), Silver (4), Gold (2), Referential (1), Quarantine (1) |
| **Airflow Pipeline Tasks** | **16 tasks** | Organized into 5 logical TaskGroups |
| **Referential Integrity Matches** | **100% (0 orphans)** | All 50,000 facts join cleanly to `dim_track`, `dim_country`, `dim_date` |
| **Bridge Weight Sums** | **1.0000** | Strict credit conservation across 130k artist & 113k genre pairings |
| **Tracks Quarantined** | **1 row** | Missing `track_name` and `duration_ms <= 0` |
| **Quarantine Error Rate** | **0.00088%** | 1 / 114,000 tracks (well under our 0.1% allowed budget) |
| **Retry Policy** | **3 retries** | 2 min initial delay, exponential backoff, max 10 min |
| **Max Task Timeout** | **45 minutes** | SLA enforcement preventing hung processes |
| **Isolated Unit Tests** | **22 automated tests** | In `data_quality/tests/`, 100% pass rate |

---

## Part 3 - Core Concepts in Plain Language

1. **What is Great Expectations (GX)?** A Python framework for translating implicit data assumptions into declarative, automated tests (Expectations) and generating visual HTML Data Docs.
2. **What is Apache Airflow?** A workflow orchestrator where workflows are codified in Python as Directed Acyclic Graphs (DAGs).
3. **What is a Quality Gate?** A blocking pipeline step that halts downstream execution if data does not satisfy contract invariants.
4. **What is Quarantine Isolation?** Isolating defective records into a separate dataset (`tracks_quarantine`) with reason metadata while allowing valid records to proceed.
