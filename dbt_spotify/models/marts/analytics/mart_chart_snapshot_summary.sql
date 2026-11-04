-- Grain: snapshot date x chart type. Shows how dense the sample is on each date, so dashboards
-- can explain gaps instead of hiding them (the chart file holds only 25 dates).
select
    d.full_date                    as chart_date,
    d.year, d.month, d.season,
    ct.chart_type,
    count(*)                       as chart_entries,
    count(distinct f.country_key)  as regions_covered,
    count(distinct f.track_key)    as distinct_tracks,
    sum(f.streams)                 as total_streams,
    round(avg(f.streams), 1)       as avg_streams
from {{ ref('fact_chart') }}     f
join {{ ref('dim_date') }}       d  on d.date_key        = f.date_key
join {{ ref('dim_chart_type') }} ct on ct.chart_type_key = f.chart_type_key
group by all
