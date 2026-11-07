{#- Northern-hemisphere meteorological season (same rule as Member 2, silver_to_gold.season_expr). -#}
{% macro season_from_month(month_expr) -%}
case
    when {{ month_expr }} in (12, 1, 2) then 'Winter'
    when {{ month_expr }} in (3, 4, 5)  then 'Spring'
    when {{ month_expr }} in (6, 7, 8)  then 'Summer'
    else 'Autumn'
end
{%- endmacro %}
