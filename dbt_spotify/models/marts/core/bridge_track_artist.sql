-- Bridge: a track can credit several artists. credit_weight = 1 / artist_count (no double counting).
-- Catalogue tracks use Silver's artist_list (';'-split). Chart-only tracks use the chart credit
-- string split on ', ' (protected names such as "Tyler, The Creator" are kept whole).
with catalogue as (
    select track_id,
           unnest(artist_list)                  as artist_name,
           generate_subscripts(artist_list, 1)  as artist_position
    from {{ ref('stg_tracks') }}
),
chart_only as (
    select track_id,
           unnest(names)                        as artist_name,
           generate_subscripts(names, 1)        as artist_position
    from (
        select t.track_id, {{ split_credit('t.artist_credit') }} as names
        from {{ ref('dim_track') }} t
        where not t.is_in_catalogue
    )
),
all_credits as (
    select track_id, trim(artist_name) as artist_name, artist_position from catalogue
    union all
    select track_id, trim(artist_name), artist_position from chart_only
),
cleaned as (
    select distinct on (track_id, lower(artist_name))
           track_id, artist_name, artist_position
    from all_credits
    where artist_name is not null and artist_name <> ''
    order by track_id, lower(artist_name), artist_position
)
select
    t.track_key,
    a.artist_key,
    c.artist_position,
    c.artist_position = 1                                  as is_primary_artist,
    1.0 / count(*) over (partition by c.track_id)          as credit_weight
from cleaned c
join {{ ref('dim_track') }}  t on t.track_id = c.track_id
join {{ ref('dim_artist') }} a on a.artist_name_key = lower(c.artist_name)
