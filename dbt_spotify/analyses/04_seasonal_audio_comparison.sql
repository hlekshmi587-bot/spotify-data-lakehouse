-- Question: Do audio characteristics change between seasons? (only groups with enough data)
-- Caveat: in this sample every Winter row is from Jan-Feb 2017 and every Spring row from Mar 2018, so season is confounded with year. Differences below are NOT proof of a seasonal effect.
select season, chart_type, chart_entries, matched_entries, data_volume,
       avg_danceability, avg_energy, avg_valence, avg_acousticness, avg_tempo, explicit_share
from {{ ref('mart_seasonal_trends') }}
order by season_order, chart_type
