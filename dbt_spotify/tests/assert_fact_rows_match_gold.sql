-- Every Gold chart entry must be in the fact table exactly once (no rows lost or created by the joins).
select 'row_count_mismatch' as problem, f.n as fact_rows, g.n as gold_rows
from (select count(*) n from {{ ref('fact_chart') }}) f
cross join (select count(*) n from {{ source('lakehouse', 'gold_enriched_chart_data') }}) g
where f.n <> g.n
