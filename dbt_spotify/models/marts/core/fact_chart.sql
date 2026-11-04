-- Fact_Chart: one row per chart entry (date x region x chart type x rank). 50,000 rows.
-- Foreign keys: date_key, country_key, track_key, chart_type_key.   Degenerate key: chart_record_id.
select
    c.chart_record_id,
    c.chart_date_id                         as date_key,
    case when c.is_global_region then 0 else dc.country_key end as country_key,
    t.track_key,
    ct.chart_type_key,
    -- measures
    c.chart_rank,
    c.streams,
    c.has_streams,
    -- descriptive / flags
    c.trend,
    c.chart_rank = 1                        as is_number_one,
    c.chart_rank <= 10                      as is_top_10,
    -- points = how close to the top (top200: 200 for rank 1 ... 1 for rank 200; viral50: 50 ... 1)
    ct.max_rank + 1 - c.chart_rank          as chart_points
from {{ ref('stg_chart_entries') }} c
join      {{ ref('dim_track') }}      t  on t.track_id = c.track_id
join      {{ ref('dim_chart_type') }} ct on ct.chart_type = c.chart_type
left join {{ ref('dim_country') }}    dc on dc.iso_alpha3 = c.country_iso_alpha3
