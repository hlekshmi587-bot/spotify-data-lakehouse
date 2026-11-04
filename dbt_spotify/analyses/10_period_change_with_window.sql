-- Question: How do average streams and valence move between the snapshot months? (window function: LAG)
-- Caveat: the snapshot months are far apart (2017-01, 2017-02, 2018-03 once tiny months are excluded), so 'change' means change between sampled months, not month-over-month.
select year_month, chart_entries, data_volume, avg_streams,
       round(avg_streams - lag(avg_streams) over (order by year_month), 1) as change_in_avg_streams,
       avg_valence,
       round(avg_valence - lag(avg_valence) over (order by year_month), 4) as change_in_valence
from {{ ref('mart_period_trends') }}
where chart_type = 'top200' and data_volume <> 'insufficient'   -- tiny months would make the deltas meaningless
order by year_month
