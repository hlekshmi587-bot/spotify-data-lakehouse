-- Question: Which genres lead the charts, and what do they sound like?
select streams_rank, genre_name, charted_tracks, weighted_streams, distinct_countries,
       avg_danceability, avg_energy, avg_valence, avg_tempo
from {{ ref('mart_genre_performance') }}
order by streams_rank
limit 10
