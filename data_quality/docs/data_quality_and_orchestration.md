# Data Quality & Workflow Orchestration Specification (Member 4)

## 1. Overview & Architectural Boundaries

This module is responsible for:
1. **Data Quality Validation** via **Great Expectations** across Bronze, Silver, Gold, and Marts layers.
2. **Pipeline Orchestration** via **Apache Airflow**.
3. **Pipeline Monitoring & Failure Alerting**.
4. **Validation Documentation & Reports**.

All Data Quality assets are maintained strictly within `data_quality/`.

---

## 2. Great Expectations Architecture

### 2.1 Quality Gates & Suites

```
[Raw Sources] ---> [Bronze Layer] ---> [Silver Layer] ---> [Gold Layer] ---> [Marts Layer]
                         |                    |                  |                 |
                   Bronze Gate          Silver Gate          Gold Gate        Integrity Gate
                   (Schema/Nulls)    (Ranges/Dedup/Dates)  (Ranks/Keys)      (FKs/Bridge Sum)
```

1. **`bronze_tracks_suite`**: Schema presence, non-null `track_id`, lineage fields (`_source_file`, `_ingested_at`, `_run_id`).
2. **`bronze_charts_suite`**: Headers valid, `rank` and `date` non-null.
3. **`bronze_countries_suite`**: ISO alpha-3 non-null, column presence.
4. **`silver_tracks_suite`**: Primary key uniqueness (`track_id`), regex `^[A-Za-z0-9]{22}$`, audio feature ranges `[0.0, 1.0]`, popularity `[0, 100]`, loudness `[-60, 10]`, tempo `(0, 300]`, duration `> 0`.
5. **`silver_charts_suite`**: Rank `1..200`, chart in `{'top200', 'viral50'}`, trend in `VALID_TRENDS`, ISO date regex, composite grain uniqueness `(date, region, chart, rank)`.
6. **`silver_countries_suite`**: Unique `iso_alpha3`, lengths 2 and 3, Namibia `NA` code recovery.
7. **`silver_track_genres_suite`**: Compound uniqueness `(track_id, genre)`.
8. **`gold_enriched_charts_suite`**: `chart_record_id` SHA-256 uniqueness, `chart_year` (2010..2030), `chart_month` (1..12), `chart_season` in 4 seasons.
9. **`gold_country_analytics_suite`**: Unique `(region, chart_type)`, positive stream counts.
10. **`referential_integrity_suite`**: Foreign keys (`fact_chart` -> `dim_track`, `dim_country`), bridge weights sum strictly to 1.0 (`credit_weight`, `weighting_factor`).
11. **`quarantine_isolation_suite`**: Quarantined records document non-empty `reject_reasons`, error rate within `0.1%` budget.

### 2.2 Strict Error Handling
Any unsupported or unrecognized expectation type will **never** silently pass with `success=True`. In strict mode, an `UnsupportedExpectationError` is raised immediately. In report generation mode, `success=False` is recorded and flagged in Data Docs.

---

## 3. Workflow Orchestration (Apache Airflow)

Codified in `data_quality/airflow/dags/spotify_pipeline.py`:

- **DAG ID:** `spotify_lakehouse_pipeline`
- **Schedule:** `@daily`, `max_active_runs = 1`
- **Resilience:** `retries = 3` with `retry_exponential_backoff = True` (2 min initial up to 10 min max), `execution_timeout = timedelta(minutes=45)`.
- **Topological Dependencies:**
  `start >> preflight >> ingestion_group >> validate_bronze >> spark_group >> dbt_group >> validate_integrity >> generate_reports >> end`
- **Hard Quality Gates:** Any validation failure raises `ValueError`, stopping downstream tasks from executing.

---

## 4. Reports & Deliverables (Isolated in data_quality/reports/)

- **HTML Data Docs:** `data_quality/reports/great_expectations_report.html`
- **Quality Summary:** `data_quality/reports/data_quality_summary.md`
- **Validation JSON:** `data_quality/reports/great_expectations_validation.json`
- **Pipeline Metrics:** `data_quality/reports/pipeline_metrics.json`
- **Monitoring Alerts:** `data_quality/reports/monitoring_alerts.log`
