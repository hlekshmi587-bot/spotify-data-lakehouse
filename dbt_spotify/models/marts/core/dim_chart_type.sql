-- Dim_ChartType: tiny, but keeps descriptions and the "has stream counts" rule in one place.
select * from (values
    (1, 'top200',  'Spotify Top 200 (daily most-streamed tracks)',        200, true),
    (2, 'viral50', 'Spotify Viral 50 (fastest-growing tracks, no counts)', 50, false)
) as t(chart_type_key, chart_type, chart_type_description, max_rank, has_stream_counts)
