-- Grain: genre. How genres perform on the charts.
-- Only catalogue tracks have genres, so this covers ~17% of chart rows.
-- A multi-genre track is shared across its genres with bridge_track_genre.weighting_factor
-- (weighted_* columns add up to the real totals; chart_entries_any is the un-weighted count).
select
    g.genre_name,
    count(distinct f.track_key)                          as charted_tracks,
    count(*)                                             as chart_entries_any,
    round(sum(b.weighting_factor), 2)                    as weighted_chart_entries,
    round(sum(f.streams * b.weighting_factor), 0)        as weighted_streams,
    round(avg(f.chart_rank), 2)                          as avg_chart_rank,
    count(distinct f.country_key) filter (where not c.is_global) as distinct_countries,
    round(avg(t.popularity), 2)                          as avg_track_popularity,
    round(avg(case when t.explicit then 1.0 else 0.0 end), 4)   as explicit_share,
    {{ audio_averages('t') }},
    rank() over (order by sum(f.streams * b.weighting_factor) desc nulls last) as streams_rank
from {{ ref('fact_chart') }}          f
join {{ ref('bridge_track_genre') }}  b on b.track_key   = f.track_key
join {{ ref('dim_genre') }}           g on g.genre_key    = b.genre_key
join {{ ref('dim_track') }}           t on t.track_key    = f.track_key
join {{ ref('dim_country') }}         c on c.country_key  = f.country_key
group by g.genre_name
