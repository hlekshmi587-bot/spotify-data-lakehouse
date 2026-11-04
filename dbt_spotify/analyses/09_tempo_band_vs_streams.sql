-- Question: Do faster or slower tracks collect more streams per chart entry?
select t.tempo_band,
       count(*) as chart_entries,
       round(avg(f.streams), 0)    as avg_streams,
       round(avg(f.chart_rank), 1) as avg_rank
from {{ ref('fact_chart') }} f
join {{ ref('dim_track') }} t on t.track_key = f.track_key
join {{ ref('dim_chart_type') }} ct on ct.chart_type_key = f.chart_type_key
where ct.chart_type = 'top200' and t.tempo_band is not null
group by t.tempo_band
order by avg_streams desc
