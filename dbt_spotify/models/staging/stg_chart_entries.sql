-- Staging: one row per chart entry, renamed/selected from Member 2's Gold enriched table.
-- No business logic here; only the columns the star schema needs.
select
    chart_record_id,
    chart_date,
    chart_date_id,
    chart_type,
    region,
    is_global_region,
    country_iso_alpha3,
    chart_rank,
    track_id,
    track_title,
    chart_artist,
    trend,
    streams,
    has_streams,
    is_track_matched,
    region_match_status
from {{ source('lakehouse', 'gold_enriched_chart_data') }}
