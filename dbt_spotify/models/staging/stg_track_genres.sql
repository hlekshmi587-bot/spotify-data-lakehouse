-- Staging: one row per (track_id, genre).
select distinct track_id, genre
from {{ source('lakehouse', 'silver_track_genres') }}
where genre is not null
