-- Every non-Global chart row must map to a real country (country_key > 0).
select chart_record_id from {{ ref('fact_chart') }} f
join {{ ref('stg_chart_entries') }} s using (chart_record_id)
where not s.is_global_region and f.country_key = 0
