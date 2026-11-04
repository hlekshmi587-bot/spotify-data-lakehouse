-- Grain: country x chart type (Global included, flagged is_global).
-- Regional popularity + audio profile. Audio / popularity columns describe ONLY chart rows whose
-- track exists in tracks.csv (matched_entries); track_attribute_coverage shows how many that is.
select
    c.country_key,
    c.country_name,
    c.iso_alpha3,
    c.continent,
    c.sub_region,
    c.is_global,
    ct.chart_type,
    count(*)                                         as chart_entries,
    count(distinct f.date_key)                       as distinct_chart_dates,
    count(distinct f.track_key)                      as distinct_tracks,
    sum(f.streams)                                   as total_streams,
    round(avg(f.streams), 1)                         as avg_streams,
    round(avg(f.chart_rank), 2)                      as avg_chart_rank,
    count(*) filter (where t.is_in_catalogue)        as matched_entries,
    round(avg(case when t.is_in_catalogue then 1.0 else 0.0 end), 4) as track_attribute_coverage,
    round(avg(t.popularity), 2)                      as avg_track_popularity,
    round(avg(case when t.explicit then 1.0 when t.explicit is not null then 0.0 end), 4) as explicit_share,
    {{ audio_averages('t') }}
from {{ ref('fact_chart') }}      f
join {{ ref('dim_country') }}     c  on c.country_key    = f.country_key
join {{ ref('dim_chart_type') }}  ct on ct.chart_type_key = f.chart_type_key
join {{ ref('dim_track') }}       t  on t.track_key      = f.track_key
group by all
