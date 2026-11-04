-- Rank must not exceed the max rank of its chart type (200 for top200, 50 for viral50).
select f.chart_record_id from {{ ref('fact_chart') }} f
join {{ ref('dim_chart_type') }} ct using (chart_type_key)
where f.chart_rank > ct.max_rank or f.chart_rank < 1
