-- Grain: track (charted tracks only). How long and how widely a track stays on the charts.
select
    t.track_key,
    t.track_id,
    t.track_name,
    t.artist_credit,
    t.primary_genre,
    t.is_in_catalogue,
    count(*)                                              as chart_entries,
    count(distinct f.date_key)                            as distinct_chart_dates,
    count(distinct f.country_key) filter (where not c.is_global) as distinct_countries,
    min(d.full_date)                                      as first_chart_date,
    max(d.full_date)                                      as last_chart_date,
    date_diff('day', min(d.full_date), max(d.full_date))  as chart_span_days,
    min(f.chart_rank)                                     as best_chart_rank,
    round(avg(f.chart_rank), 2)                           as avg_chart_rank,
    count(*) filter (where f.is_number_one)               as times_number_one,
    sum(f.streams)                                        as total_streams
from {{ ref('fact_chart') }}  f
join {{ ref('dim_track') }}   t on t.track_key   = f.track_key
join {{ ref('dim_date') }}    d on d.date_key    = f.date_key
join {{ ref('dim_country') }} c on c.country_key = f.country_key
group by t.track_key, t.track_id, t.track_name, t.artist_credit, t.primary_genre, t.is_in_catalogue
