-- The weights of one track must add up to 1 in both bridges, otherwise streams get double counted.
select 'genre' as bridge, track_key, sum(weighting_factor) as total
from {{ ref('bridge_track_genre') }} group by track_key having abs(sum(weighting_factor) - 1) > 1e-9
union all
select 'artist', track_key, sum(credit_weight)
from {{ ref('bridge_track_artist') }} group by track_key having abs(sum(credit_weight) - 1) > 1e-9
