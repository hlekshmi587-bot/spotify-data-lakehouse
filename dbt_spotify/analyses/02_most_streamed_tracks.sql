-- Question: Which tracks have the most total streams across all countries?
select track_name, artist_credit, primary_genre, distinct_countries, distinct_chart_dates,
       best_chart_rank, total_streams
from {{ ref('mart_track_longevity') }}
order by total_streams desc nulls last
limit 10
