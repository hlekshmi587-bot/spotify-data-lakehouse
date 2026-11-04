-- Dim_Date: a full calendar (every day of every year that has chart data).
-- date_key = yyyymmdd, identical to Member 2's chart_date_id, so it joins without conversion.
with bounds as (
    select min(chart_date) as min_date, max(chart_date) as max_date
    from {{ ref('stg_chart_entries') }}
),
spine as (
    select cast(unnest(generate_series(
               cast(date_trunc('year', min_date) as date),
               cast(date_trunc('year', max_date) + interval 1 year - interval 1 day as date),
               interval 1 day)) as date) as full_date
    from bounds
)
select
    cast(strftime(full_date, '%Y%m%d') as integer)  as date_key,
    full_date,
    year(full_date)                                 as year,
    quarter(full_date)                              as quarter,
    month(full_date)                                as month,
    strftime(full_date, '%B')                       as month_name,
    strftime(full_date, '%Y-%m')                    as year_month,
    day(full_date)                                  as day_of_month,
    cast(weekofyear(full_date) as integer)          as week_of_year,
    cast(isodow(full_date) as integer)              as day_of_week_number,   -- 1 = Monday
    strftime(full_date, '%A')                       as day_of_week_name,
    isodow(full_date) >= 6                          as is_weekend,
    {{ season_from_month('month(full_date)') }}     as season
from spine
