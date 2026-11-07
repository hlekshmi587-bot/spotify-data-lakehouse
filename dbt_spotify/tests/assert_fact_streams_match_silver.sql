-- Total streams in the star schema must equal the Silver chart table (nothing dropped or doubled).
select f.s as fact_streams, s.s as silver_streams
from (select sum(streams) s from {{ ref('fact_chart') }}) f
cross join (select sum(streams) s from {{ source('lakehouse', 'silver_charts') }}) s
where f.s is distinct from s.s
