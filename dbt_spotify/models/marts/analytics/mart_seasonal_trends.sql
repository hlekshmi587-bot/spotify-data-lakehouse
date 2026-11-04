-- Grain: season x chart type  (seasonal pattern; seasons pooled over all years).
-- WARNING: ~99.9% of Top 200 rows come from just 7 snapshot dates (Jan-Feb 2017, Mar 2018);
-- Viral 50 has only 22 rows in total, so some groups are tiny. data_volume flags this:
--   'ok' >= 5000 entries | 'limited' 1000-4999 | 'insufficient' < 1000 (do not draw conclusions).
select
    d.season,
    case d.season when 'Winter' then 1 when 'Spring' then 2 when 'Summer' then 3 else 4 end as season_order,
    ct.chart_type,
    count(*)                                   as chart_entries,
    count(distinct d.full_date)                as distinct_chart_dates,
    count(distinct f.track_key)                as distinct_tracks,
    count(distinct f.country_key)              as distinct_regions,
    sum(f.streams)                             as total_streams,
    round(avg(f.streams), 1)                   as avg_streams,
    count(*) filter (where t.is_in_catalogue)  as matched_entries,
    round(avg(case when t.is_in_catalogue then 1.0 else 0.0 end), 4) as track_attribute_coverage,
    case when count(*) >= 5000 then 'ok' when count(*) >= 1000 then 'limited' else 'insufficient' end as data_volume,
    round(avg(t.popularity), 2)                as avg_track_popularity,
    round(avg(case when t.explicit then 1.0 when t.explicit is not null then 0.0 end), 4) as explicit_share,
    {{ audio_averages('t') }}
from {{ ref('fact_chart') }}     f
join {{ ref('dim_date') }}       d  on d.date_key       = f.date_key
join {{ ref('dim_chart_type') }} ct on ct.chart_type_key = f.chart_type_key
join {{ ref('dim_track') }}      t  on t.track_key      = f.track_key
group by d.season, ct.chart_type
