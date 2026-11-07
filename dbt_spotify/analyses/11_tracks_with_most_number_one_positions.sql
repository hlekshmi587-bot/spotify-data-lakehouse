-- Question: Which tracks reached #1 most often?
select track_name, artist_credit, times_number_one, distinct_countries, total_streams
from {{ ref('mart_track_longevity') }}
where times_number_one > 0
order by times_number_one desc, total_streams desc nulls last
limit 10
