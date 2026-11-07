-- Question: Which artists charted in the most countries, and how many streams are attributed to them?
select reach_rank, artist_name, distinct_countries, distinct_tracks, best_chart_rank,
       attributed_streams, streams_rank
from {{ ref('mart_artist_leaderboard') }}
order by reach_rank
limit 10
