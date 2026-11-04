-- Dim_Country: all 249 ISO countries/territories + one row for the worldwide "Global" chart.
-- Global is a chart, not a country, so it has no ISO code; country_key = 0 and is_global = true.
-- BI queries that compare countries must filter is_global = false.
with countries as (
    select
        row_number() over (order by iso_alpha3)    as country_key,
        iso_alpha3                                  as country_code,
        iso_alpha3,
        iso_alpha2,
        country_name,
        coalesce(continent,           'Unknown')    as continent,
        coalesce(sub_region,          'Unknown')    as sub_region,
        intermediate_region,
        geography_source,
        false                                       as is_global
    from {{ ref('stg_countries') }}
),
global_row as (
    select 0 as country_key, 'GLOBAL' as country_code, cast(null as varchar) as iso_alpha3,
           cast(null as varchar) as iso_alpha2, 'Global' as country_name,
           'Worldwide' as continent, 'Worldwide' as sub_region,
           cast(null as varchar) as intermediate_region,
           'not_applicable' as geography_source, true as is_global
)
select * from global_row
union all
select * from countries
