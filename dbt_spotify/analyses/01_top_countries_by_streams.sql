-- Question: Which countries generate the most Top 200 streams (in the sampled dates)?
select country_name, continent, chart_entries, distinct_tracks, total_streams, avg_streams
from {{ ref('mart_country_profile') }}
where not is_global and chart_type = 'top200'
order by total_streams desc
limit 10
