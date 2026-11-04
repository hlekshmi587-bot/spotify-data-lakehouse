{#- Split a ", "-separated artist credit string into a list of artist names.
    Names that legitimately contain ", " (var protected_artist_names) are protected first,
    like Member 2's split_artists() in spark/silver_to_gold.py. -#}
{% macro split_credit(column) -%}
{%- set ns = namespace(expr=column) -%}
{%- for name in var('protected_artist_names') -%}
    {%- set ns.expr = "replace(" ~ ns.expr ~ ", '" ~ name.replace("'", "''") ~ "', '" ~ name.replace(", ", "@@").replace("'", "''") ~ "')" -%}
{%- endfor -%}
list_transform(string_split({{ ns.expr }}, ','), lambda x: trim(replace(x, '@@', ', ')))
{%- endmacro %}
