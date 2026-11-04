-- Grain: country x track. Top 10 tracks per country on the Top 200 chart, ranked by total streams
-- over the sampled snapshot dates (NOT lifetime streams: the source is a sample of 25 dates).
with per_track as (
    select
        f.country_key,
        f.track_key,
        count(*)                       as chart_entries,
        count(distinct f.date_key)     as dates_charted,
        min(f.chart_rank)              as best_rank,
        round(avg(f.chart_rank), 2)    as avg_rank,
        sum(f.streams)                 as total_streams
    from {{ ref('fact_chart') }} f
    join {{ ref('dim_chart_type') }} ct on ct.chart_type_key = f.chart_type_key
    where ct.chart_type = 'top200'
    group by f.country_key, f.track_key
),
ranked as (
    select *, row_number() over (partition by country_key order by total_streams desc, best_rank, track_key) as streams_rank
    from per_track
)
select
    c.country_name, c.continent, c.sub_region, c.is_global,
    r.streams_rank,
    t.track_name, t.artist_credit, t.primary_genre, t.is_in_catalogue,
    r.chart_entries, r.dates_charted, r.best_rank, r.avg_rank, r.total_streams,
    r.country_key, r.track_key
from ranked r
join {{ ref('dim_country') }} c on c.country_key = r.country_key
join {{ ref('dim_track') }}   t on t.track_key    = r.track_key
where r.streams_rank <= 10
