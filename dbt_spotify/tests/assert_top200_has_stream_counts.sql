-- Business rule from Member 2's profiling: only Viral 50 rows may lack streams; every Top 200 row must have them.
select f.chart_record_id from {{ ref('fact_chart') }} f
join {{ ref('dim_chart_type') }} ct using (chart_type_key)
where ct.chart_type = 'top200' and f.streams is null
