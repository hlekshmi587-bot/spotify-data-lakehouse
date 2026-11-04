-- Question: In which countries do we know the audio features of most chart tracks? (how far to trust audio results)
select country_name, chart_entries, matched_entries, track_attribute_coverage
from {{ ref('mart_country_profile') }}
where not is_global and chart_type = 'top200' and chart_entries >= 500   -- skip countries with very few chart rows
order by track_attribute_coverage desc
limit 10
