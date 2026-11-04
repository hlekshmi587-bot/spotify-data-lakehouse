-- Question: How common are explicit tracks on the charts in each continent?
select continent,
       sum(matched_entries) as matched_entries,
       round(sum(explicit_share * matched_entries) / sum(matched_entries), 3) as explicit_share
from {{ ref('mart_country_profile') }}
where not is_global and chart_type = 'top200' and matched_entries > 0
group by continent
order by explicit_share desc
