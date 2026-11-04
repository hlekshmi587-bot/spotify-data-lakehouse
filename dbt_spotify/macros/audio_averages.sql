{#- Average of every Spotify audio feature of a track-dimension alias.
    Unmatched chart rows have NULL audio features and AVG() ignores NULLs, so these
    averages describe only chart rows whose track exists in tracks.csv. -#}
{% macro audio_averages(alias, prefix='avg_') -%}
{%- for col in ['danceability','energy','speechiness','acousticness','instrumentalness','liveness','valence','loudness','tempo'] %}
    round(avg({{ alias }}.{{ col }}), 4) as {{ prefix }}{{ col }}{{ "," if not loop.last }}
{%- endfor %}
{%- endmacro %}
