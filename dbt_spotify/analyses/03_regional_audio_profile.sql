-- Question: How do audio characteristics differ between UN sub-regions? (weighted by matched chart rows)
-- Country averages are re-weighted by matched_entries so a big country counts more than a small one.
select sub_region,
       count(*)                                                        as countries,
       sum(matched_entries)                                            as matched_entries,
       round(sum(avg_danceability * matched_entries) / sum(matched_entries), 3) as danceability,
       round(sum(avg_energy       * matched_entries) / sum(matched_entries), 3) as energy,
       round(sum(avg_valence      * matched_entries) / sum(matched_entries), 3) as valence,
       round(sum(avg_acousticness * matched_entries) / sum(matched_entries), 3) as acousticness,
       round(sum(avg_tempo        * matched_entries) / sum(matched_entries), 1) as tempo_bpm
from {{ ref('mart_country_profile') }}
where not is_global and chart_type = 'top200' and matched_entries > 0
group by sub_region
having sum(matched_entries) >= 300      -- ignore sub-regions with too little audio data
order by energy desc
