-- Dim_Track: every track that appears on a chart OR in the catalogue.
--   * 89,740 catalogue tracks (full audio features, from Silver)
--   * charted tracks that are NOT in tracks.csv (~5.7k) get a row with name/artist from the chart
--     and NULL audio features, so the fact table never has an orphan track_key.
-- is_in_catalogue tells analysts which rows have audio data.
with catalogue as (
    select
        track_id, track_name, artists as artist_credit, primary_artist, artist_count, album_name,
        primary_genre, genre_count, popularity, duration_ms, explicit,
        danceability, energy, key, loudness, mode, speechiness, acousticness,
        instrumentalness, liveness, valence, tempo, time_signature,
        true as is_in_catalogue
    from {{ ref('stg_tracks') }}
),
chart_only as (
    select
        track_id,
        min(track_title)  as track_name,        -- deterministic pick if titles differ across rows
        min(chart_artist) as artist_credit
    from {{ ref('stg_chart_entries') }}
    where track_id not in (select track_id from catalogue)
    group by track_id
),
unioned as (
    select * from catalogue
    union all
    select
        track_id, track_name, artist_credit,
        {{ split_credit('artist_credit') }}[1] as primary_artist,
        len({{ split_credit('artist_credit') }}) as artist_count,
        cast(null as varchar) as album_name, cast(null as varchar) as primary_genre,
        cast(null as integer) as genre_count, cast(null as integer) as popularity,
        cast(null as bigint) as duration_ms, cast(null as boolean) as explicit,
        cast(null as double) as danceability, cast(null as double) as energy,
        cast(null as integer) as key, cast(null as double) as loudness, cast(null as integer) as mode,
        cast(null as double) as speechiness, cast(null as double) as acousticness,
        cast(null as double) as instrumentalness, cast(null as double) as liveness,
        cast(null as double) as valence, cast(null as double) as tempo,
        cast(null as integer) as time_signature,
        false as is_in_catalogue
    from chart_only
)
select
    row_number() over (order by track_id) as track_key,
    track_id,
    track_name,
    artist_credit,
    primary_artist,
    artist_count,
    album_name,
    primary_genre,
    genre_count,
    popularity,
    duration_ms,
    round(duration_ms / 60000.0, 2) as duration_min,
    explicit,
    danceability, energy, key, loudness, mode, speechiness, acousticness,
    instrumentalness, liveness, valence, tempo, time_signature,
    -- Derived mood label (valence x energy, threshold 0.5). Common convention, NOT a Spotify field.
    case
        when valence is null or energy is null then null
        when valence >= 0.5 and energy >= 0.5 then 'Happy / Energetic'
        when valence <  0.5 and energy >= 0.5 then 'Tense / Angry'
        when valence >= 0.5 and energy <  0.5 then 'Calm / Peaceful'
        else 'Sad / Melancholic'
    end as mood_quadrant,
    case
        when tempo is null then null
        when tempo < 90  then 'Slow (<90 BPM)'
        when tempo < 120 then 'Medium (90-119 BPM)'
        when tempo < 150 then 'Fast (120-149 BPM)'
        else 'Very fast (150+ BPM)'
    end as tempo_band,
    is_in_catalogue
from unioned
