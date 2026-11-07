-- Bridge: a track can have several genres (many-to-many). weighting_factor = 1 / genre_count,
-- so that SUM(measure * weighting_factor) over the bridge does NOT double-count a track's streams.
select
    t.track_key,
    g.genre_key,
    1.0 / count(*) over (partition by t.track_key) as weighting_factor
from {{ ref('stg_track_genres') }} sg
join {{ ref('dim_track') }} t on t.track_id = sg.track_id
join {{ ref('dim_genre') }} g on g.genre_name = sg.genre
