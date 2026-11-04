-- Dim_Genre: one row per genre label found in the catalogue (114).
select
    row_number() over (order by genre) as genre_key,
    genre                              as genre_name
from (select distinct genre from {{ ref('stg_track_genres') }})
