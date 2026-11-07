-- Grain: artist. Credited artists ranked by attributed streams and by international reach.
-- credit_weight splits a track's streams evenly between its credited artists (no double counting);
-- chart_entries counts every entry where the artist is credited (a feat. track counts for each artist).
select
    a.artist_key,
    a.artist_name,
    count(*)                                              as chart_entries,
    count(distinct f.track_key)                           as distinct_tracks,
    count(distinct f.date_key)                            as distinct_chart_dates,
    count(distinct f.country_key) filter (where not c.is_global) as distinct_countries,
    min(f.chart_rank)                                     as best_chart_rank,
    round(avg(f.chart_rank), 2)                           as avg_chart_rank,
    count(*) filter (where f.is_number_one)               as times_number_one,
    round(sum(f.streams * b.credit_weight), 0)            as attributed_streams,
    rank() over (order by sum(f.streams * b.credit_weight) desc nulls last)     as streams_rank,
    rank() over (order by count(distinct f.country_key) filter (where not c.is_global) desc,
                          sum(f.streams * b.credit_weight) desc nulls last)     as reach_rank
from {{ ref('fact_chart') }}          f
join {{ ref('bridge_track_artist') }} b on b.track_key  = f.track_key
join {{ ref('dim_artist') }}          a on a.artist_key = b.artist_key
join {{ ref('dim_country') }}         c on c.country_key = f.country_key
group by a.artist_key, a.artist_name
