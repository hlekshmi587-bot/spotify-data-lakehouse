-- Dim_Artist: one row per distinct artist NAME (case-insensitive).
-- Sources have no artist id, so two different artists with the same name collapse into one row,
-- and artist_country / artist_genre cannot be filled (documented limitation).
-- Defined from dim_track so it contains exactly the artists that the bridge can reference.
with names as (
    select unnest(artist_list) as artist_name
    from {{ ref('stg_tracks') }}
    union all
    select unnest({{ split_credit('artist_credit') }}) as artist_name
    from {{ ref('dim_track') }}
    where not is_in_catalogue
),
cleaned as (
    select lower(trim(artist_name)) as artist_name_key,
           min(trim(artist_name))   as artist_name
    from names
    where artist_name is not null and trim(artist_name) <> ''
    group by 1
)
select
    row_number() over (order by artist_name_key) as artist_key,
    artist_name_key,
    artist_name
from cleaned
