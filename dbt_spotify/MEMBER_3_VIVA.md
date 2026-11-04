# Member 3 - viva preparation
**My part:** "I designed the analytical data model and implemented dbt transformations and SQL analytics."

## 30-second summary
Member 2 gave me Parquet Silver/Gold tables. I modelled them as a star schema (1 fact, 4 dimensions, 2 bridges) with dbt on DuckDB,
built 8 analytics marts for regional, temporal, seasonal, genre and artist questions, wrote 12 SQL queries, and protected it with 102 tests
(including reconciliation against Member 2's Gold). `dbt build`: 123/123 passed.

## Likely questions and honest answers
1. **Why a star schema if Gold already has wide tables?** Wide tables answer one question each. A star schema has one fact and shared (conformed) dimensions, so any question
   (country x season x genre x artist) uses the same joins, BI tools understand it, and attributes are defined once.
2. **What is the grain of the fact table?** One chart entry: date x region x chart type x rank (50,000 rows). Every measure and key is at that grain.
3. **Why dbt?** SQL transformations become versioned, ordered by dependency (`ref()`), tested and documented; lineage is generated automatically.
4. **Why DuckDB, not Spark?** The lakehouse is local Parquet; DuckDB queries it in place, needs no cluster or Hadoop setup, and builds in seconds. The model is plain SQL; only the adapter and a few functions would change for Spark.
5. **What are bridge tables?** A track has many genres and many artists (many-to-many). A bridge holds the pairs. Joining it multiplies fact rows, so each pair has a weight (1/n) that sums to 1 per track; `sum(streams * weight)` stays correct. A test enforces the sum.
6. **Why are there rows in `dim_track` that are not in the catalogue?** Only 17.11% of chart rows match `tracks.csv`. Dropping the rest would delete 83% of facts. They get a row with NULL audio features and `is_in_catalogue = false`.
7. **Why is Global in `dim_country`?** It is a chart, not a country; a NULL foreign key would break joins. Key 0 plus `is_global`; country comparisons filter it out.
8. **Surrogate vs natural keys?** Surrogate integers for compact joins; natural keys kept for traceability. Caveat: surrogates are regenerated each build.
9. **SCD?** Not needed: the data are snapshots with no attribute history, so Type 1 is enough.
10. **How do you know the model is right?** Fact rows = Gold rows; total streams = Silver; mart_country_profile equals Member 2's country_analytics for all groups (compared on ISO code, full outer join, so a missing group also fails); no orphan keys; weights sum to 1. I also fixed my own first reconciliation test, which joined on country names and silently skipped 9 of 73 groups.
11. **What do the results show?** Valid statements: Drake/Weeknd/Ed Sheeran lead attributed streams; "dance", "pop" and "house" lead genres; Latin America charts are higher in danceability and valence than East Asia. All with the 17% audio coverage caveat.
12. **Is there a seasonal effect?** **No conclusion can be drawn.** 99.9% of Top 200 rows come from 7 dates; Winter is essentially Jan-Feb 2017 and Spring essentially Mar 2018, so season and year are confounded; Summer/Autumn have almost no rows. The marts show the method, the `data_volume` flag marks weak groups.
13. **Difference between my dbt tests and Member 4's Great Expectations?** dbt tests check the *model* (keys, relationships, reconciliation, ranges) at build time; Great Expectations validates *data files/layers* with profiles and reports and is orchestrated by Airflow. Complementary.
14. **What would you do with more time/data?** Fuller chart history (makes seasonality testable), artist ids, incremental models and a Spark/Databricks adapter, snapshots for slowly changing attributes.
15. **What are the weaknesses?** Artists identified by name only; mood/tempo bands are conventions; local DuckDB only; thin sample.
