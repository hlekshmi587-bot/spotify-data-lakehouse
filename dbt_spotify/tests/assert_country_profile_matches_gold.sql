-- Reconcile our mart with Member 2's Gold country_analytics (region x chart_type).
-- Compared on ISO code (Member 2's `region` keeps the chart's own spelling, e.g. "UK"; ours is the
-- official name). FULL OUTER JOIN so a group missing on EITHER side also fails the test.
with mart as (
    select coalesce(iso_alpha3, 'GLOBAL') as code, chart_type,
           sum(chart_entries) as chart_entries, sum(total_streams) as total_streams
    from {{ ref('mart_country_profile') }} group by 1, 2
),
gold as (
    select coalesce(country_iso_alpha3, 'GLOBAL') as code, chart_type,
           sum(chart_entries) as chart_entries, sum(total_streams) as total_streams
    from {{ source('lakehouse', 'gold_country_analytics') }} group by 1, 2
)
select coalesce(m.code, g.code) as code, coalesce(m.chart_type, g.chart_type) as chart_type,
       m.chart_entries as mart_entries, g.chart_entries as gold_entries,
       m.total_streams as mart_streams, g.total_streams as gold_streams
from mart m
full outer join gold g on g.code = m.code and g.chart_type = m.chart_type
where m.code is null or g.code is null
   or m.chart_entries <> g.chart_entries
   or m.total_streams is distinct from g.total_streams
