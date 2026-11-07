-- Question: What mood (valence x energy) do charted tracks have, per chart type?
select ct.chart_type, t.mood_quadrant,
       count(*) as chart_entries,
       round(100.0 * count(*) / sum(count(*)) over (partition by ct.chart_type), 1) as pct_of_matched_entries
from {{ ref('fact_chart') }} f
join {{ ref('dim_track') }} t on t.track_key = f.track_key
join {{ ref('dim_chart_type') }} ct on ct.chart_type_key = f.chart_type_key
where t.mood_quadrant is not null and ct.chart_type = 'top200'   -- viral50 has only 1 matched row in this sample
group by ct.chart_type, t.mood_quadrant
order by ct.chart_type, chart_entries desc
